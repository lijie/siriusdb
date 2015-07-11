package main

import (
	proto "code.google.com/p/goprotobuf/proto"
	"errors"
	"net"
	sirius_net "seasun.com/sirius/net"
	PbCenter "seasun.com/sirius/proto/center.pb"
	PbCommon "seasun.com/sirius/proto/common.pb"
	"time"
)

var ErrCenterUexpectedResp = errors.New("center response error")

type CenterNotifier interface {
	Notify(cmd uint32, msg proto.Message)
}

func centerSendRegister(conn net.Conn) {
	// send register
	var reg PbCenter.RegReq
	var addr PbCommon.Addr

	addr.Ip = proto.String("0.0.0.0")
	addr.Port = proto.Uint32(0)
	addr.Aid = proto.Int(-1)
	addr.Sid = proto.Int(-1)
	addr.Gid = proto.Int64(-1)

	reg.Type = proto.Int(8)
	reg.Addr = &addr
	// keep dbflushd only 1
	reg.Resv = proto.Int(1)
	logger.Println(reg)
	sirius_net.SendMsg(conn, &reg, uint32(PbCenter.Command_CMD_REG_REQ), 1)
}

func centerSendNotifyReg(conn net.Conn) {
	// send notifyreg
	var req2 PbCenter.RegNotifyReq
	req2.Type = proto.Int(4)
	sirius_net.SendMsg(conn, &req2, uint32(PbCenter.Command_CMD_REG_NOTIFY_REQ), 2)
}

func centerSendGetServerReq(conn net.Conn) {
	// send getserver
	var req3 PbCenter.GetServerReq
	req3.Type = proto.Int(4)
	req3.Aid = proto.Int(-1)
	req3.Sid = proto.Int(-1)
	sirius_net.SendMsg(conn, &req3, uint32(PbCenter.Command_CMD_GET_SERVER_REQ), 3)
}

func procCenterResp(conn net.Conn, h *sirius_net.Header, b []byte, ch chan proto.Message) error {
	if h.Err != 0 {
		logger.Printf("ERROR: center resp unexpected error %d\n", h.Err)
		return ErrCenterUexpectedResp
	}

	// register ok, get dbproxy list
	if h.Cmd == uint32(PbCenter.Command_CMD_REG_RSP) {
		centerSendGetServerReq(conn)
		return nil
	}

	if h.Cmd == uint32(PbCenter.Command_CMD_NOTIFY_REQ) {
		centerSendGetServerReq(conn)
		return nil
	}

	if h.Cmd == uint32(PbCenter.Command_CMD_GET_SERVER_RSP) {
		var rsp PbCenter.GetServerRsp
		if err := proto.Unmarshal(b, &rsp); err != nil {
			return err
		}
		ch <- &rsp
	}
	return nil
}

func centerLoop(dest string, ch chan proto.Message) {
	var err error
	var conn net.Conn

	// connect
	for {
		conn, err = net.Dial("tcp", dest)
		if err != nil {
			logger.Printf("dial center err %v, sleep and reconnect\n", err)
			time.Sleep(5 * time.Second)
			continue
		}

		centerSendRegister(conn)
		centerSendNotifyReg(conn)

		var header sirius_net.Header
		var b [64 * 1024]byte
		var n int
		for {
			n, err = sirius_net.Recv(conn, &header, b[0:])
			if err != nil {
				logger.Printf("ERROR: sirius_net recv err %v\n", err)
				break
			}
			err = procCenterResp(conn, &header, b[0:n], ch)
			if err != nil {
				logger.Printf("ERROR: sirius_net resp err %v\n", err)
				break
			}
		}

		conn.Close()
		time.Sleep(time.Minute)
	}

	// wait loop
	return
}
