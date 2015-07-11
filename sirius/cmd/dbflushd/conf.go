package main

import (
	"fmt"
	"github.com/aarzilli/golua/lua"
)

//type DBAddr struct {
//	DBName    string
//	RedisAddr string
//}

type Conf struct {
	Center    string
	RedisAddr []string
}

func LoadConf(path string, c *Conf) error {
	L := lua.NewState()
	defer L.Close()
	L.OpenLibs()

	err := L.DoFile(path)
	if err != nil {
		return err
	}

	L.GetGlobal("CENTER_IP")
	L.GetGlobal("CENTER_PORT")
	p := L.ToInteger(-1)
	s := L.ToString(-2)
	c.Center = fmt.Sprintf("%s:%d", s, p)
	L.Pop(2)

	L.GetGlobal("dbproxy")
	L.GetField(-1, "database")
	L.PushNil()

	// read database
	for L.Next(-2) != 0 {
		L.GetField(-1, "redis_url")
		L.PushNil()

		for L.Next(-2) != 0 {
			c.RedisAddr = append(c.RedisAddr, L.ToString(-1))
			L.Pop(1)
		}

		L.Pop(2)
	}
	return nil
}
