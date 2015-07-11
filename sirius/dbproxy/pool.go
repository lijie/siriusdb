// simple connection pool
package dbproxy

import (
	"io"
)

type Pool struct {
	New    func() interface{}
	connch chan io.Closer
}

func (p *Pool) Get() interface{} {
	var c io.Closer
	var ok bool

	select {
	case c, ok = <-p.connch:
	default:
		return p.New()
	}

	if !ok {
		return nil
	}

	return c
}

func (p *Pool) Put(c io.Closer) {
	select {
	case p.connch <- c:
	default:
		c.Close()
	}
}

func NewPool(capcity int, factory func() interface{}) *Pool {
	return &Pool{
		New:    factory,
		connch: make(chan io.Closer, capcity),
	}
}
