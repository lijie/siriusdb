package main

import (
	proto "code.google.com/p/goprotobuf/proto"
	"container/list"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	sirius_net "seasun.com/sirius/net"
	dbproto "seasun.com/sirius/proto/dbproxy.pb"
	immortaldb "seasun.com/sirius/proto/immortaldb.pb"
	"sync"
	"time"
)

type request struct {
	start    time.Time
	seq      uint32
	callback func(header *sirius_net.Header, body []byte)
	elem     *list.Element
}

var reqmap map[uint32]*request
var reqlist *list.List
var reqlock sync.Mutex
var enableCPUProfiler bool

func recvloop(conn *net.TCPConn, count int, done chan int) {
	buffer := sirius_net.NewBuffer(conn)
	var req *request
	var ok bool

	if enableCPUProfiler {
		f, _ := os.OpenFile("/tmp/go_profile", os.O_CREATE|os.O_RDWR, 0666)
		pprof.StartCPUProfile(f)
	}

	delayFast := 0
	delay50 := 0
	delay100 := 0
	delay500 := 0
	delaySlow := 0
	var delay int64

	for count > 0 {
		err := buffer.ReadMsg(nil)
		if err != nil {
			break
		}
		count--

		reqlock.Lock()
		req, ok = reqmap[buffer.Header.Seq]
		delete(reqmap, buffer.Header.Seq)
		reqlock.Unlock()
		if !ok {
			log.Printf("unknown seq %d\n", buffer.Header.Seq)
			continue
		}

		delay = time.Now().Sub(req.start).Nanoseconds()
		if delay > 1000000000 {
			delaySlow++
		} else if delay > 500000000 {
			delay500++
		} else if delay > 100000000 {
			delay100++
		} else if delay > 50000000 {
			delay50++
		} else {
			delayFast++
		}

		req.callback(&buffer.Header, buffer.Body())
	}

	fmt.Printf("delay stat %d %d %d %d %d\n", delayFast, delay50, delay100, delay500, delaySlow)
	if enableCPUProfiler {
		pprof.StopCPUProfile()
	}
	done <- 1
}

var dbAddr string
var enableRead bool
var enableWrite bool
var extendSize int
var testCount int
var dbName string
var requestSeq = 0
var sendSpeed int

func nextseq() uint32 {
	requestSeq++
	return uint32(requestSeq)
}

func sendUseDB(conn net.Conn) error {
	var usedb dbproto.UseDB
	usedb.Dbname = proto.String(dbName)

	err := sirius_net.SendMsg(conn, &usedb, uint32(dbproto.Command_CMD_USEDB_REQ), 1)
	if err != nil {
		return err
	}

	var header sirius_net.Header
	var body [64]byte
	_, err = sirius_net.Recv(conn, &header, body[0:])
	if err != nil {
		return err
	}

	if header.Err != 0 {
		log.Fatal("use db err %v\n", err)
	}
	return nil
}

func writePlayer(conn net.Conn, buffer *sirius_net.Buffer, seq uint32) error {
	player := &immortaldb.Player{
		Account: proto.String("testaccount"),
		Name:    proto.String("testname"),
		Level:   proto.Uint32(1),
		Exp:     proto.Uint32(100),
		Extend:  make([]byte, extendSize),
		Roleid:  proto.Uint64(uint64(seq)),
	}
	set := &dbproto.Set{
		MsgType: proto.String("Player"),
		Key:     proto.String(fmt.Sprintf("%d", seq)),
		//Flags:   proto.Uint32(uint32(Flags_FLAG_CACHE_ONLY)),
	}
	set.Value, _ = proto.Marshal(player)
	err := buffer.WriteMsg(set, uint32(dbproto.Command_CMD_SET_REQ), seq)
	assert(err == nil)
	return err
}

func readPlayer(conn net.Conn, buffer *sirius_net.Buffer, seq uint32) error {
	get := &dbproto.Get{
		MsgType: proto.String("Player"),
		Key:     proto.String(fmt.Sprintf("%d", seq)),
		//Flags:   proto.Uint32(uint32(Flags_FLAG_CACHE_ONLY)),
	}
	err := buffer.WriteMsg(get, uint32(dbproto.Command_CMD_GET_REQ), seq)
	if err != nil {
		fmt.Println(err)
	}
	assert(err == nil)
	return err
}

func writeData(conn net.Conn) {
	var req *request
	buffer := sirius_net.NewBuffer(conn)
	for i := 0; i < testCount; i++ {
		req = &request{
			start: time.Now(),
			seq:   uint32(i),
			callback: func(h *sirius_net.Header, b []byte) {
				if h.Err != 0 {
					fmt.Printf("write req %d error %d\n", h.Seq, h.Err)
				}
			},
		}
		reqlock.Lock()
		reqmap[uint32(i)] = req
		reqlock.Unlock()
		writePlayer(conn, buffer, uint32(i))
	}
}

func readData(conn net.Conn) {
	var req *request
	buffer := sirius_net.NewBuffer(conn)
	for i := 0; i < testCount; i++ {
		req = &request{
			start: time.Now(),
			seq:   uint32(i),
			callback: func(h *sirius_net.Header, b []byte) {
				if h.Err != 0 {
					fmt.Printf("read req %d error %d\n", h.Seq, h.Err)
				}
			},
		}
		reqlock.Lock()
		reqmap[uint32(i)] = req
		reqlock.Unlock()
		readPlayer(conn, buffer, uint32(i))

		if i > 0 && i%sendSpeed == 0 {
			time.Sleep(1 * time.Second)
		}
	}
}

func writeDataSlow(conn net.Conn, countPerSecond int) {
}

func main() {
	flag.StringVar(&dbAddr, "dbaddr", "127.0.0.1:30100", "dbproxy addr")
	flag.BoolVar(&enableRead, "read", true, "enable read test")
	flag.BoolVar(&enableWrite, "write", true, "enable write test")
	flag.IntVar(&extendSize, "size", 1024, "test body size")
	flag.IntVar(&testCount, "count", 1024, "test read/write count")
	flag.IntVar(&sendSpeed, "speed", 2000, "send speed per second")
	flag.StringVar(&dbName, "dbname", "test_for_dba", "set database name")
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	fmt.Printf("connect %s\n", dbAddr)
	addr, err := net.ResolveTCPAddr("tcp", dbAddr)
	assert(err == nil)
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		fmt.Printf("connect err %v\n", err)
	}
	assert(err == nil)

	done := make(chan int)
	reqmap = make(map[uint32]*request)

	sendUseDB(conn)

	if enableWrite {
		go recvloop(conn, testCount, done)
		now := time.Now()
		writeData(conn)
		_ = <-done
		assert(len(reqmap) == 0)
		fmt.Printf("Write done, time cost %s\n", time.Now().Sub(now).String())
	}

	if enableRead {
		go recvloop(conn, testCount, done)
		now := time.Now()
		readData(conn)
		_ = <-done
		assert(len(reqmap) == 0)
		fmt.Printf("Read done, time cost %s\n", time.Now().Sub(now).String())
	}
}

func assert(cond bool) {
	if !cond {
		_, f, l, _ := runtime.Caller(1)
		log.Fatal("assert failed:\n", f, l)
	}
}
