package main

//
// flush
// * 使用pipeline从redis的 <dbname>_dbprox_flush_list 获取待刷新的items
// * 获得items后, 从redis删除掉已经获得的数据避免其它dbflush进程重复flush
// * 利用items信息从redis获得待刷新的data, 并去重
// * 将数据发送给dbproxy, 写入db
// * 启动 proxyLoop(), 获取写db的结果
// * 对于返回错误的写请求, 将对应的item保存到err_items
// * proxyLoop()完成后, 如果err_items不会空,
//   则将err_items内的数据append到flush_list, 等待下次flush
//

import (
	proto "code.google.com/p/goprotobuf/proto"
	"errors"
	"flag"
	"github.com/garyburd/redigo/redis"
	"log"
	"net"
	"os"
	sirius_net "seasun.com/sirius/net"
	PbCenter "seasun.com/sirius/proto/center.pb"
	// PbCommon "seasun.com/sirius/proto/common.pb"
	"bytes"
	"fmt"
	"hash/crc32"
	"os/signal"
	"runtime/pprof"
	dbproto "seasun.com/sirius/proto/dbproxy.pb"
	"sync"
	"syscall"
	"time"
)

var ErrorConn = errors.New("connection error")
var ErrorNoData = errors.New("no data")
var ErrorUseDB = errors.New("use db error")
var logger *log.Logger
var logfile logFile

const (
	FLUSH_LIST  = "dbproxy_flush_list"
	FLUSH_START = 0
	FLUSH_END   = 200
)

type FlushStat struct {
	start        time.Time
	end          time.Time
	itemCount    int
	flushCount   int
	errorCount   int
	invalidCount int
}

func (fs *FlushStat) Init() {
	fs.itemCount = 0
	fs.flushCount = 0
	fs.errorCount = 0
	fs.invalidCount = 0
}

type FlushTask struct {
	redis_conn []redis.Conn
	proxy_conn []net.Conn
	nextseq    uint32
	reqmap     map[uint32]*dbproto.FlushItem
	reqchan    chan int
	err_items  []*dbproto.FlushItem
	count      int
	dbname     string
	listname   string
	// protect reqmap
	lock sync.Mutex
	// protect run
	runlock sync.Mutex
	done    chan int
	done2   chan int
	taskid  int
	alive   bool
	conf    *FlushConf
	stat    FlushStat
}

func (task *FlushTask) proxyLoop(done chan error) {
	var header sirius_net.Header
	var err error
	var item *dbproto.FlushItem
	var ok bool
	var b [64 * 1024]byte

	for {
		err = nil
		_, more := <-task.reqchan
		if !more {
			break
		}

		_, err = sirius_net.Recv(task.proxy_conn[0], &header, b[0:])
		if err != nil {
			break
		}

		task.lock.Lock()
		item, ok = task.reqmap[header.Seq]
		task.lock.Unlock()
		if !ok {
			logger.Print("unknown seq:", header.Seq)
			task.stat.invalidCount++
			continue
		}

		if header.Err != 0 {
			task.err_items = append(task.err_items, item)
			task.stat.errorCount++
			logger.Printf("proxy flush %s return err %d", item.GetKey(), header.Err)
		}
	}

	logger.Printf("proxyLoop exit")
	task.stat.end = time.Now()

	// output stat
	logger.Printf("Stat of last task:")
	logger.Printf("%#v", task.stat)
	done <- err
}

func (task *FlushTask) useDB() error {
	var usedb dbproto.UseDB
	usedb.Dbname = proto.String(task.dbname)

	err := sirius_net.SendMsg(task.proxy_conn[0], &usedb, uint32(dbproto.Command_CMD_USEDB_REQ), 1)
	if err != nil {
		return err
	}

	var header sirius_net.Header
	var b [64]byte
	_, err = sirius_net.Recv(task.proxy_conn[0], &header, b[0:])
	if err != nil {
		return err
	}

	if header.Err != 0 {
		logger.Print("use db err:", err)
		return ErrorUseDB
	}
	return nil
}

func (task *FlushTask) doFlush(values [][]byte, items []dbproto.FlushItem) error {
	var err error
	var set dbproto.Set

	if err = task.useDB(); err != nil {
		return err
	}

	startrecv := false
	itemmap := map[string]int{}
	done := make(chan error, 1)

	for i := 0; i < len(values); i++ {
		// no data, ignore
		if values[i] == nil {
			continue
		}

		dumpkey := items[i].GetMsgType() + "_" + items[i].GetKey()
		// dumplicated, ignore
		if _, ok := itemmap[dumpkey]; ok {
			continue
		}
		task.stat.flushCount++
		itemmap[dumpkey] = 1

		set.MsgType = items[i].MsgType
		set.Flags = proto.Uint32(uint32(dbproto.Flags_FLAG_DB_ONLY))
		set.Value = values[i]
		set.Key = items[i].Key
		err = sirius_net.SendMsg(task.proxy_conn[0], &set, uint32(dbproto.Command_CMD_SET_REQ), task.nextseq)
		if err != nil {
			// network err
			return err
		}
		if !startrecv {
			go task.proxyLoop(done)
			startrecv = true
		}

		task.lock.Lock()
		task.reqmap[task.nextseq] = &items[i]
		task.lock.Unlock()
		// logger.Printf("send %v\n", set)
		task.nextseq++
		task.reqchan <- 1
	}

	close(task.reqchan)

	// wait recv done
	if startrecv {
		err = <-done
	}
	return err
}

func (task *FlushTask) appendItem(item *dbproto.FlushItem) error {
	b, err := proto.Marshal(item)
	if err != nil {
		logger.Print("ERRAPPEND:", item)
		return err
	}
	_, err = task.redis_conn[0].Do("RPUSH", task.listname, b)
	if err != nil {
		logger.Print("ERRAPPEND:", item)
		return ErrorConn
	}
	return nil
}

func (task *FlushTask) appendUnflushedItems(items []dbproto.FlushItem) error {
	var err error
	for i := 0; i < len(items); i++ {
		err = task.appendItem(&items[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (task *FlushTask) appendErrItems() error {
	var err error
	for i := 0; i < len(task.err_items); i++ {
		err = task.appendItem(task.err_items[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (task *FlushTask) trimItemsFromRedis() error {
	var err error
	r := task.redis_conn[0]
	if err = r.Send("LTRIM", task.listname, task.count, -1); err != nil {
		return err
	}
	if err = r.Flush(); err != nil {
		return err
	}
	if _, err = r.Receive(); err != nil {
		logger.Print("LTRIM err:", err)
		return err
	}
	return nil
}

func (task *FlushTask) readItemsFromRedis() ([]interface{}, error) {
	r := task.redis_conn[0]
	var err error
	err = r.Send("LRANGE", task.listname, FLUSH_START, FLUSH_END)
	if err != nil {
		return nil, err
	}
	// send command
	if err = r.Flush(); err != nil {
		logger.Print("Redis Flush err:", err)
		return nil, err
	}
	return redis.Values(r.Receive())
}

type byteSlice []byte

func (b byteSlice) Less(i, j int) bool {
	return i > j
}

func (b byteSlice) Len() int {
	return len(b)
}

func (b byteSlice) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (task *FlushTask) getRedisConn(key string) redis.Conn {
	b := bytes.NewBufferString(key)
	b.WriteString("turing")

	idx := crc32.ChecksumIEEE(b.Bytes()) % uint32(len(task.redis_conn))
	return task.redis_conn[idx]
}

func (task *FlushTask) readValuesFromRedis(items []dbproto.FlushItem) ([][]byte, error) {
	// TODO:
	// use MGET for performace
	count := len(items)
	values := make([][]byte, count)
	var err error
	var conn redis.Conn
	for i := 0; i < count; i++ {
		redis_key := items[i].GetMsgType() + "_" + items[i].GetKey()
		conn = task.getRedisConn(items[i].GetKey())
		values[i], err = redis.Bytes(conn.Do("GET", redis_key))
		if err != nil {
			logger.Printf("Read Key %s but ERROR: redis: %v", redis_key, err)
			values[i] = nil

			// if no data from redis, just ignore
			if err == redis.ErrNil {
				continue
			}

			// if other errors, return
			return nil, err
		}
	}
	return values, nil
}

func (task *FlushTask) Do() error {
	// clear err_items
	task.err_items = make([]*dbproto.FlushItem, 0)
	// clear reqmap
	task.reqmap = make(map[uint32]*dbproto.FlushItem, 0)

	reply, err := task.readItemsFromRedis()
	if reply == nil || len(reply) == 0 {
		// logger.Print("no data, sleep 10s")
		time.Sleep(10 * time.Second)
		return err
	}
	task.stat.Init()
	task.stat.start = time.Now()
	task.stat.itemCount = len(reply)

	count := len(reply)
	// alloc chan
	task.reqchan = make(chan int, count)
	task.count = count

	// unmarshal all items
	items := make([]dbproto.FlushItem, count)
	for i := 0; i < count; i++ {
		proto.Unmarshal(reply[i].([]byte), &items[i])
	}

	values, err := task.readValuesFromRedis(items)
	if err != nil {
		logger.Printf("Stop flush, read values Error: %v", err)
		return err
	}

	// logger.Print("do flush\n")

	// flush to db
	err = task.doFlush(values, items)
	if err != nil {
		logger.Printf("Stop flush, flush Error: %v", err)
		return err
	}

	// logger.Print("append\n")

	// append error item to flush list
	err = task.appendErrItems()
	if err != nil {
		logger.Printf("Stop flush, append Error: %v", err)
		return err
	}

	err = task.trimItemsFromRedis()
	if err != nil {
		logger.Printf("Stop flush, trim Error: %v", err)
		return err
	}
	return nil
}

func (task *FlushTask) Notify(cmd uint32, msg proto.Message) {
	// logger.Debug(msg)
}

func (task *FlushTask) CloseConn() {
	for i := range task.redis_conn {
		if task.redis_conn[i] != nil {
			task.redis_conn[i].Close()
			task.redis_conn[i] = nil
		}
	}
	for i := range task.proxy_conn {
		if task.proxy_conn[i] != nil {
			task.proxy_conn[i].Close()
			task.proxy_conn[i] = nil
		}
	}
}

func (task *FlushTask) Run(delay time.Duration) error {
	if delay != 0 {
		time.Sleep(delay * time.Second)
	}
	run := true
	task.runlock.Lock()
	task.alive = true
	task.runlock.Unlock()

	fc := task.conf
	logger.Print("Run task:", task.taskid, fc)

	defer func() {
		// clear resource if return
		task.CloseConn()
		task.done <- task.taskid
		task.alive = false
		logger.Print("Stop task:", task.taskid, fc)
	}()

	if len(fc.proxyAddr) == 0 {
		logger.Print("no proxy addr found")
		return nil
	}

	// connect to redis
	task.redis_conn = make([]redis.Conn, len(fc.redisAddr))
	for i := range fc.redisAddr {
		logger.Print("connect redis:", fc.redisAddr[i])
		c, err := redis.Dial("tcp", fc.redisAddr[i])
		if err != nil {
			logger.Print("dial redis err:", err)
			return err
		}
		task.redis_conn[i] = c
	}

	// connect to proxy
	task.proxy_conn = make([]net.Conn, len(fc.proxyAddr))
	for i := range fc.proxyAddr {
		c, err := net.Dial("tcp", fc.proxyAddr[i])
		if err != nil {
			logger.Printf("connect proxy %v err %v\n", fc.proxyAddr[i], err)
			return err
		}
		task.proxy_conn[i] = c
	}

	logger.Print("Connect done, start flush loop")
	var err error
	// flush loop
	for run {
		select {
		case _ = <-task.done2:
			// close by other
			logger.Print("Stop task by channel")
			run = false
		default:
			if err = task.Do(); err != nil {
				// close if error
				logger.Print("Stop task by error:", err)
				run = false
				break
			}
		}
	}

	return err
}

func (task *FlushTask) Restart() {
	task.runlock.Lock()
	defer task.runlock.Unlock()
	if task.alive {
		// close
		task.done2 <- 1
	}
}

func NewTask(conf *FlushConf, id int, taskch chan int) *FlushTask {
	return &FlushTask{
		taskid:   id,
		done:     taskch,
		done2:    make(chan int, 10),
		conf:     conf,
		dbname:   conf.dbName,
		listname: FLUSH_LIST,
	}
}

var logPath string
var confPath string
var serverConf Conf
var enableCPUProfiler bool

type FlushConf struct {
	redisAddr []string
	dbName    string
	proxyAddr []string
}

func updateProxyAddr(fc []FlushConf, rsp *PbCenter.GetServerRsp) {
	logger.Printf("updateProxyAddr:\n%v", rsp)

	addrs := rsp.GetAddrs()
	if len(addrs) == 0 {
		return
	}
	proxyaddr := make([]string, len(addrs))
	for i := range addrs {
		a := addrs[i].GetAddr()
		proxyaddr[i] = fmt.Sprintf("%s:%d", a.GetIp(), a.GetPort())
	}

	for i := range fc {
		fc[i].proxyAddr = proxyaddr
	}

	logger.Print("new proxy config:", fc)
}

func nextRotate() time.Duration {
	y, m, d := time.Now().Date()
	today := time.Date(y, m, d, 0, 0, 0, 0, time.Local)
	return today.Add(24 * time.Hour).Sub(time.Now())
}

func mainLoop(fc []FlushConf) {
	ch := make(chan proto.Message, 10)
	sig_ch := make(chan os.Signal, 10)

	signal.Notify(sig_ch, syscall.SIGTERM)
	if enableCPUProfiler {
		f, _ := os.OpenFile("/tmp/dbflush_profile", os.O_CREATE|os.O_RDWR, 0666)
		pprof.StartCPUProfile(f)
	}
	go centerLoop(serverConf.Center, ch)

	// init task
	tasklist := make([]*FlushTask, len(fc))
	taskch := make(chan int, len(fc))
	for i := 0; i < len(fc); i++ {
		task := NewTask(&fc[i], i, taskch)
		tasklist[i] = task
		go task.Run(10)
	}

	tick_ch := time.Tick(nextRotate())

	for {
		select {
		case p := <-ch:
			updateProxyAddr(fc, p.(*PbCenter.GetServerRsp))
			for i := range tasklist {
				tasklist[i].Restart()
			}
		case _ = <-sig_ch:
			if enableCPUProfiler {
				pprof.StopCPUProfile()
			}
			os.Exit(0)
		case id := <-taskch:
			logger.Printf("task %d stopped, restart\n", id)
			go tasklist[id].Run(10)
		case _ = <-tick_ch:
			logfile.rotate = true
			tick_ch = time.Tick(nextRotate())
		}
	}
}

type logFile struct {
	file   *os.File
	rotate bool
}

func (lf *logFile) openNewFile() error {
	var err error
	year, month, day := time.Now().Date()
	if err = os.MkdirAll(logPath, 0755); err != nil {
		panic("")
	}
	lf.file, err = os.OpenFile(fmt.Sprintf("%s/dbflushd-%04d-%02d-%02d.log",
		logPath, year, month, day),
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("openfile error %v\n", err)
		panic("")
	}
	return err
}

func (lf *logFile) Write(b []byte) (int, error) {
	if lf.rotate {
		// fmt.Printf("start rotate...")
		lf.file.Close()
		lf.openNewFile()
		lf.rotate = false
	}
	return lf.file.Write(b)
}

func initLog() {
	logfile.openNewFile()
	logger = log.New(&logfile, "", log.LstdFlags|log.Lshortfile)
}

func main() {
	flag.StringVar(&logPath, "log", ".", "log file path")
	flag.StringVar(&confPath, "conf", "./config.lua", "conf file path")
	flag.BoolVar(&enableCPUProfiler, "cpuprofiler", false, "enable cpu profiler")
	flag.Parse()

	initLog()
	logger.Print("Start...")

	// load lua config
	LoadConf(confPath, &serverConf)

	// convert lua config to FlushConf
	fc := make([]FlushConf, 1)
	fc[0].redisAddr = serverConf.RedisAddr
	// fc[0].proxyAddr
	// fc[0].dbName = serverConf.DB[i].DBName

	logger.Print("init FlushConf:\n%v", fc)
	for {
		mainLoop(fc)
	}
}
