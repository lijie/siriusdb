package dbproxy

import (
	"database/sql"
	"errors"
	_ "fmt"
	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	"hash/crc32"
	"io"
	"reflect"
	sirius_rpc "seasun.com/sirius/net/rpc"
	dbproto "seasun.com/sirius/proto/dbproxy.pb"
)

var ErrNoData = errors.New("no data")

type protoService struct {
	name          string
	typMap        map[string]*objType
	splitTable    map[string]*SplitTable
	cacheConnPool *Pool
	dbConnPool    *Pool
}

type objType struct {
	typ   reflect.Type
	index string
}

type Config struct {
	CenterURL string
	Dbproxy   DbproxyConfig
}

type DbproxyConfig struct {
	Name       string
	DbMod      uint32
	DbURL      []string
	CacheMod   uint32
	CacheURL   []string
	SplitTable []SplitTable
}

func (s *protoService) Register(obj interface{}, index string) error {
	ot := &objType{
		typ:   reflect.TypeOf(obj),
		index: index,
	}

	if _, ok := s.typMap[ot.typ.Name()]; ok {
		return errors.New("protobuf service: type already registerd")
	}

	s.typMap[ot.typ.Name()] = ot
	return nil
}

func protoStructForScan(u interface{}) []interface{} {
	val := reflect.ValueOf(u).Elem()
	v := make([]interface{}, val.NumField()-1)
	for i := 0; i < val.NumField()-1; i++ {
		v[i] = val.Field(i).Addr().Interface()
	}
	return v
}

func (s *protoService) hashtable(key, table string) string {
	if sp, ok := s.splitTable[table]; !ok {
		return table
	} else {
		return sp.tableName[crc32.ChecksumIEEE([]byte(key))%sp.Mod]
	}
}

func (s *protoService) dbGet(table, index, key string, val interface{}) error {
	conn := s.dbConnPool.Get()
	if conn == nil {
		return errors.New("connect to mysql failed")
	}
	defer s.dbConnPool.Put(conn.(io.Closer))

	return conn.(*sql.DB).QueryRow("SELECT * FROM ? WHERE ? = ? LIMIT 1",
		s.hashtable(key, table), index, key).Scan(protoStructForScan(val)...)
}

func (s *protoService) cacheGet(msgtype, key string) (interface{}, error) {
	conn := s.cacheConnPool.Get()
	if conn == nil {
		return nil, errors.New("connect to redis failed")
	}
	defer s.cacheConnPool.Put(conn.(io.Closer))
	return conn.(redis.Conn).Do("GET", msgtype+"_"+key)
}

func (s *protoService) Get(arg interface{}) (interface{}, int) {
	val := arg.(*dbproto.Get)
	msgtype := val.GetMsgType()

	reply, err := s.cacheGet(msgtype, val.GetKey())
	if err == nil && reply != nil {
		return reply, 0
	}
	if err != nil {
		return nil, int(dbproto.Errcode_ERR_RWCACHE)
	}

	typ, ok := s.typMap[msgtype]
	if !ok {
		return nil, int(dbproto.Errcode_ERR_UNKNOWMSG)
	}

	reply2 := reflect.New(typ.typ)
	if err = s.dbGet(msgtype, typ.index, val.GetKey(), reply2.Interface()); err != nil {
		return nil, int(dbproto.Errcode_ERR_RWDB)
	}
	return reply2.Interface(), 0
}

func (s *protoService) Set(arg interface{}) (interface{}, int) {
	return nil, 0
}

type SplitTable struct {
	TableName string
	Hash      string
	Mod       uint32
	tableName []string
}

type ServiceParams struct {
	Name       string
	CacheAddr  string
	DBAddr     string
	SplitTable []SplitTable
}

func newProtoService(rs *sirius_rpc.Server) *protoService {
	return nil
}
