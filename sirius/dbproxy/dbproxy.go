package dbproxy

import (
	// proto "code.google.com/p/goprotobuf/proto"
	_ "errors"
	"io"
	"net"
	sirius_rpc "seasun.com/sirius/net/rpc"
)

type Server struct {
	rpcService rpcService
	service    service
}

type cacheService interface {
	Get(key string, val interface{}) error
	Set(key string, val interface{}) error
	Del(key string) error
}

type rpcService interface {
	ServeConn(rwc io.ReadWriteCloser)
	Register(cmd uint32, rcvr interface{}, method interface{}) error
}

type service interface {
	Get(arg interface{}) (interface{}, int)
	Set(arg interface{}) (interface{}, int)
	Register(obj interface{}, index string) error
}

func (s *Server) ListenAndServe(protocol, addr string) error {
	l, err := net.Listen(protocol, addr)
	if err != nil {
		return err
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}

		go s.rpcService.ServeConn(conn)
	}

	return nil
}

func NewServer() *Server {
	rs := sirius_rpc.NewServer()
	return &Server{
		rpcService: rs,
		service:    newProtoService(rs),
	}
}

//func Test() {
//	server := &Server{make(map[string]*objType)}
//	server.RegisterProtobuf(onlinedbproto.AccountTable{}, "name")
//
//	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:3306)/test_for_dba")
//	if err != nil {
//		log.Fatal("db open err: ", err)
//		return
//	}
//	rows, err := db.Query("SELECT * FROM AccountTable LIMIT 5")
//	if err != nil {
//		log.Fatal("db exec err: ", err)
//		return
//	}
//	defer rows.Close()
//	fmt.Println(rows)
//
//	columns, err := rows.Columns()
//	if err != nil {
//		log.Fatal(err)
//		return
//	}
//
//	fmt.Println(columns)
//
//	for rows.Next() {
//		var val onlinedbproto.AccountTable
//		err = rows.Scan(protoStructForScan(&val)...)
//		if err != nil {
//			log.Fatal("scan ", err)
//			return
//		}
//		fmt.Println(*val.Name)
//	}
//}
