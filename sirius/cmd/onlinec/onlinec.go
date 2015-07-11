//
// Online 测试工具
// TestFunc1:
// 创建账户,角色
// 设置角色在线
// 登录到GS
// 重复登录
// 同角色再次设置在线被踢
// 重连后登录
//
// TestFunc2:
// 创建第二个角色
// 更改角色名字和extend
// 用第一角色的连接登录第二角色, 应该失败
// 第二角色登录, 第一角色被踢
//
// TestFunc3:
// 最大角色数量限制
// 更改账户名
//

package main

import (
	proto "code.google.com/p/goprotobuf/proto"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	_ "os"
	"reflect"
	"runtime"
	sirius_net "seasun.com/sirius/net"
	sirius_rpc "seasun.com/sirius/net/rpc"
	gsproto "seasun.com/sirius/proto/gs.pb"
	onlineproto "seasun.com/sirius/proto/online.pb"
	onlinedbproto "seasun.com/sirius/proto/onlinedb.pb"
	"time"
)

var ErrNoAccount = errors.New("account does not exist")

var accountName string
var roleName string
var roleName2 string

var onlineAddr string
var gsAddr string

var onlineConn net.Conn
var gsConn net.Conn
var gsConn2 net.Conn

var buffer *sirius_net.Buffer

type Server struct {
	Conn   net.Conn
	Client *sirius_rpc.Client
}

var onlineServer Server
var gsServer1 Server
var gsServer2 Server

type RoleInfo onlinedbproto.RoleInfo

func (r *RoleInfo) Maaaa() {
	a := (*onlinedbproto.RoleInfo)(r)
	a.Name = proto.String("hello")
	fmt.Println(*a)
}

func test() {
	var ri onlinedbproto.RoleInfo
	t := reflect.TypeOf(ri)
	fmt.Println(t)
	fmt.Println(t.NumField())
	fmt.Println(t.Name())

	for i := 0; i < t.NumField(); i++ {
		fmt.Printf("field name %s\n", t.Field(i).Name)
		fmt.Printf("field name %v\n", t.Field(i).Type.Kind())
		fmt.Printf("field name %v\n", t.Field(i).Type.Elem().Kind())
		fmt.Printf("\ttag value %s\n", t.Field(i).Tag.Get("protobuf"))
	}

	var a RoleInfo
	a.Maaaa()
}

func connect(dest string) (net.Conn, error) {
	// connect online
	addr, err := net.ResolveTCPAddr("tcp", dest)
	checkError(err)
	conn, err := net.DialTCP("tcp", nil, addr)
	checkError(err)
	return conn, nil
}

// func sendAndRecv(conn net.Conn, cmd uint32, req proto.Message, rsp proto.Message) (int, error) {
// 	err := buffer.WriteMsg(conn, req, cmd, 1)
// 	if err != nil {
// 		return 0, err
// 	}
// 	// os.Exit(10)
// 	err = buffer.ReadMsg(conn, rsp)
// 	if err != nil {
// 		return int(buffer.Header.Err), err
// 	}
// 	return int(buffer.Header.Err), nil
// }

func sendAndRecv(s *Server, cmd uint32, req proto.Message, rsp proto.Message) error {
	return s.Client.Call(cmd, req, rsp)
}

func getAccountInfo() (*onlineproto.GetAccountInfoRsp, error) {
	req := &onlineproto.GetAccountInfoReq{
		Account: proto.String(accountName),
	}
	var rsp onlineproto.GetAccountInfoRsp
	err := sendAndRecv(&onlineServer, uint32(onlineproto.Command_CMD_GETACCOUNTINFO_REQ), req, &rsp)
	// no data
	if err != nil && err.Error() == "server error 10106" {
		return nil, ErrNoAccount
	}
	if err != nil {
		log.Fatal("online err", err)
	}
	return &rsp, nil
}

func createRole(name string) (uint64, error) {
	req := &onlineproto.CreateRoleReq{
		Account: proto.String(accountName),
		Name:    proto.String(name),
		Aid:     proto.Uint32(0),
		Sid:     proto.Uint32(0),
	}
	var rsp onlineproto.CreateRoleRsp
	err := sendAndRecv(&onlineServer, uint32(onlineproto.Command_CMD_CREATEROLE_REQ), req, &rsp)
	// fmt.Printf("create role %s but err %v\n", name, err)
	// assert(err == nil)
	if err != nil {
		return 0, err
	}
	return uint64(rsp.GetRoleid()), err
}

func setRoleOnline(roleid uint64) error {
	req := &onlineproto.SetOnlineReq{
		Account: proto.String(accountName),
		Name:    proto.String(roleName),
		Gid:     proto.Int64(9151314442816878372),
		Token:   []byte{0x44, 0x45, 0x46, 0x47},
		Roleid:  proto.Uint64(roleid),
	}
	var rsp onlineproto.SetOnlineRsp
	return sendAndRecv(&onlineServer, uint32(onlineproto.Command_CMD_SET_ONLINE_REQ), req, &rsp)
}

func rename(roleid uint64, name string) error {
	req := &onlineproto.RenameReq{
		Account: proto.String(accountName),
		Name:    proto.String(roleName + "Rename"),
		Roleid:  proto.Uint64(roleid),
	}
	var rsp onlineproto.RenameRsp
	return sendAndRecv(&onlineServer, uint32(onlineproto.Command_CMD_RENAME_REQ), req, &rsp)
}

func loginGS(s *Server, name string, roleid uint64) error {
	req := &gsproto.LoginReq{
		Account: proto.String(accountName),
		Name:    proto.String(name),
		Token:   []byte{0x44, 0x45, 0x46, 0x47},
		Roleid:  proto.Uint64(roleid),
	}
	var rsp gsproto.LoginRsp
	return sendAndRecv(s, uint32(gsproto.Command_CMD_LOGIN_REQ), req, &rsp)
}

func loginGS1(roleid uint64) error {
	return loginGS(&gsServer1, roleName, roleid)
}

func loginGS2(roleid uint64) error {
	return loginGS(&gsServer2, roleName2, roleid)
}

func TestFunc3() {
	fmt.Printf("start TestFunc3...\n")
	// 获取账户信息
	var roleid uint64
	accountInfo, err := getAccountInfo()
	if err == ErrNoAccount {
		roleid, err = createRole(roleName2)
	}
	checkError(err)
	if len(accountInfo.Accounts) <= 1 {
		roleid, err = createRole(roleName2)
		checkError(err)
	} else {
		// find role
		for i := range accountInfo.Accounts {
			ac := accountInfo.Accounts[i]
			if ac.GetName() == roleName2 {
				roleid = ac.GetRoleid()
			}
		}
		// not found
		if roleid == 0 {
			roleid, err = createRole(roleName2)
			checkError(err)
		}
	}
	fmt.Println(accountInfo)
	fmt.Println(roleid)
	assert(roleid != 0)
	// 更改角色名称
	err = rename(roleid, roleName2+"XXX")
	assert(err == nil)
	err = rename(roleid, roleName2)
	assert(err == nil)

	// 最大角色数量限制
	pass := false
	for i := 0; i < 256; i++ {
		name := fmt.Sprintf("%s%d", "ShouldFail", time.Now().UnixNano())
		_, err = createRole(name)
		if err != nil && err.Error() == "server error 10818" {
			pass = true
			break
		}
	}
	assert(pass)
}

func TestFunc2() {
	fmt.Printf("start TestFunc2...")
	// 获取账户信息
	var roleid uint64
	accountInfo, err := getAccountInfo()
	if err == ErrNoAccount {
		roleid, err = createRole(roleName2)
	}
	checkError(err)
	if len(accountInfo.Accounts) <= 1 {
		roleid, err = createRole(roleName2)
		checkError(err)
	} else {
		// find role
		for i := range accountInfo.Accounts {
			ac := accountInfo.Accounts[i]
			if ac.GetName() == roleName2 {
				roleid = ac.GetRoleid()
			}
		}
		// not found
		if roleid == 0 {
			roleid, err = createRole(roleName2)
			checkError(err)
		}
	}
	fmt.Println(accountInfo)
	fmt.Println(roleid)
	assert(roleid != 0)

	// 设置roleid在线
	err = setRoleOnline(roleid)
	if err != nil {
		fmt.Println(err)
	}
	assert(err == nil)
	fmt.Printf("set online done\n")

	// online应该不运行同一账户的2个角色同时在线
	// 所以此时之前登录的角色应该已经被踢, 连接应该已经断开
	// login to gs
	// 失败 连接应该被关闭
	err = loginGS(&gsServer1, roleName2, roleid)
	assert(err != nil)
	fmt.Printf("login should faile %v\n", err)
	// gsConn.Close()
	gsServer1.Client.Close()

	// login to gs
	// 成功
	// gsConn2, _ = connect(gsAddr)
	err = loginGS2(roleid)
	assert(err == nil)
	fmt.Printf("login should ok\n")

	// 更改角色信息
	// TODO
}

func TestFunc1() {
	fmt.Printf("start TestFunc1...")
	// 获取账户信息
	fmt.Printf("Fetch user account info\n")
	var roleid uint64
	var errcode int
	accountInfo, err := getAccountInfo()
	if err == ErrNoAccount {
		roleid, err = createRole(roleName)
		fmt.Printf("create role %s\n", roleName)
		checkError(err)
		accountInfo, err = getAccountInfo()
		checkError(err)
	}

	if len(accountInfo.Accounts) == 0 {
		roleid, err = createRole(roleName)
		fmt.Printf("create role %s\n", roleName)
		checkError(err)
	} else {
		// find role
		for i := range accountInfo.Accounts {
			ac := accountInfo.Accounts[i]
			if ac.GetName() == roleName {
				roleid = ac.GetRoleid()
			}
		}
		// not found
		if roleid == 0 {
			roleid, err = createRole(roleName)
			fmt.Printf("create role %s\n", roleName)
			checkError(err)
		}
	}
	fmt.Println(accountInfo)
	fmt.Println(roleid)

	// 设置roleid在线
	fmt.Printf("try set %d online...\n", roleid)
	err = setRoleOnline(roleid)
	assert(err == nil)
	fmt.Printf("set online done\n")

	// login to gs
	fmt.Printf("try login %d to gs...\n", roleid)
	err = loginGS1(roleid)
	fmt.Printf("errcode %d, err %v\n", errcode, err)
	assert(err == nil)
	fmt.Printf("login ok\n")

	// 重复登录
	fmt.Printf("try login %d repeat...\n", roleid)
	err = loginGS1(roleid)
	assert(err.Error() == "server error 10503")
	fmt.Printf("login repeat done\n")

	// 重复设置roleid在线
	// 之前登录GS的role应该被踢下线
	err = setRoleOnline(roleid)
	assert(err == nil)
	fmt.Printf("set online again done\n")

	// 再次发登录信息
	// 应该发送失败, 连接已经被GS断开
	err = loginGS1(roleid)
	assert(err != nil)
	fmt.Println("loginGS1 should failed, err:", err)

	// 重新连接gs
	// gsConn.Close()
	// gsConn, _ = connect(gsAddr)
	gsServer1.Client.Close()
	gsServer1.Conn, _ = connect(gsAddr)
	gsServer1.Client = sirius_rpc.NewClient(gsServer1.Conn)

	// login to gs
	err = loginGS1(roleid)
	assert(err == nil)
	fmt.Printf("login ok\n")
}

func main() {
	flag.StringVar(&onlineAddr, "online", "127.0.0.1:30800", "online server addr")
	flag.StringVar(&gsAddr, "gs", "127.0.0.1:30500", "game server addr")
	flag.Parse()

	var err error

	accountName = fmt.Sprintf("%s%d", "TestOnline", time.Now().UnixNano())
	roleName = fmt.Sprintf("%s%d", "TestRole", time.Now().UnixNano())
	roleName2 = fmt.Sprintf("%s%d", "TestRole2", time.Now().UnixNano())

	onlineServer.Conn, err = connect(onlineAddr)
	assert(err == nil)
	onlineServer.Client = sirius_rpc.NewClient(onlineServer.Conn)

	gsServer1.Conn, err = connect(gsAddr)
	assert(err == nil)
	gsServer1.Client = sirius_rpc.NewClient(gsServer1.Conn)

	gsServer2.Conn, err = connect(gsAddr)
	assert(err == nil)
	gsServer2.Client = sirius_rpc.NewClient(gsServer2.Conn)

	TestFunc1()
	TestFunc2()
	TestFunc3()
}

func checkError(err error) {
	if err != nil {
		_, f, l, _ := runtime.Caller(1)
		log.Fatal(err, f, l)
	}
}

func assert(cond bool) {
	if !cond {
		_, f, l, _ := runtime.Caller(1)
		log.Fatal("assert failed:\n", f, l)
	}
}
