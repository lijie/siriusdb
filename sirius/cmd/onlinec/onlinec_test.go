package main

import (
	"fmt"
	sirius_rpc "seasun.com/sirius/net/rpc"
	"testing"
	"time"
)

var testOnlineAddr = "127.0.0.1:30800"

// Do nothing
func ExampleOnline() {
	fmt.Printf("OnlineExample\n")
	// Output:
	// OnlineExample
}

func connectOnline(t *testing.T) {
	var err error
	onlineAddr = "127.0.0.1:30800"
	onlineServer.Conn, err = connect(onlineAddr)
	if err != nil {
		t.Fatal("connect online", err)
	}
	onlineServer.Client = sirius_rpc.NewClient(onlineServer.Conn)
}

// TestCreateRole
func TestCreateRole(t *testing.T) {
	connectOnline(t)

	accountName = fmt.Sprintf("%s%d", "TestOnline", time.Now().UnixNano())
	_, err := getAccountInfo()
	if err != ErrNoAccount {
		t.Fatal("account ", accountName, " already exists")
	}

	// assume max role is 3
	for i := 0; i < 3; i++ {
		start := time.Now().UnixNano()
		rolename := fmt.Sprintf("%s%d", "role", start+int64(i))
		_, err := createRole(rolename)
		if err != nil {
			t.Fatal("create role", rolename, err)
		}
	}

	onlineServer.Client.Close()
}
