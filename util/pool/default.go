package pool

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/goneric/stack"
	"go-micro.dev/v4/transport"
)

type pool struct {
	size int
	ttl  time.Duration
	tr   transport.Transport

	sync.Mutex
	conns map[string]stack.Stack[*poolConn]
}

type poolConn struct {
	transport.Client
	id      string
	created time.Time
}

func newPool(options Options) *pool {
	return &pool{
		size:  options.Size,
		tr:    options.Transport,
		ttl:   options.TTL,
		conns: make(map[string]stack.Stack[*poolConn]),
	}
}

func (p *pool) Close() error {
	var err error

	p.Lock()
	for _, conns := range p.conns {
		for conns.Size() > 0 {
			if conn, ok := conns.Pop(); ok {
				if nerr := conn.Client.Close(); nerr != nil {
					err = nerr
				}
			}
		}
	}
	p.Unlock()

	return err
}

// NoOp the Close since we manage it.
func (p *poolConn) Close() error {
	return nil
}

func (p *poolConn) Id() string {
	return p.id
}

func (p *poolConn) Created() time.Time {
	return p.created
}

func (p *pool) Get(addr string, opts ...transport.DialOption) (Conn, error) {
	p.Lock()
	conns, ok := p.conns[addr]
	p.Unlock()
	if !ok {
		conns = stack.New[*poolConn]()
	}

	// While we have conns check age and then return one
	// otherwise we'll create a new conn
	for conns.Size() > 0 {
		conn, ok := conns.Pop()
		if !ok {
			continue
		}

		// Push it back in front if this is another connection
		if conn.Remote() != addr {
			conns.Push(conn)
			continue
		}

		// If conn is old kill it and move on
		if d := time.Since(conn.Created()); d > p.ttl {
			if err := conn.Client.Close(); err != nil {
				p.Lock()
				p.conns[addr] = conns
				p.Unlock()

				return nil, err
			}

			continue
		}

		p.Lock()
		p.conns[addr] = conns
		p.Unlock()

		return conn, nil
	}

	// create new conn
	c, err := p.tr.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}

	return &poolConn{
		Client:  c,
		id:      uuid.New().String(),
		created: time.Now(),
	}, nil
}

func (p *pool) Release(conn Conn, err error) error {
	switch c := conn.(type) {
	case *poolConn:
		p.Lock()
		conns, ok := p.conns[conn.Remote()]
		p.Unlock()

		if !ok {
			conns = stack.New[*poolConn]()
		}

		// logger.Tracef("[%s] (%d/%d) conns", c.Remote(), conns.Size(), p.size)

		// don't store the conn if it has errored
		if err != nil {
			return c.Client.Close()
		}

		if conns.Size() >= p.size {
			return c.Client.Close()
		}

		conns.Push(c)

		p.Lock()
		p.conns[conn.Remote()] = conns
		p.Unlock()
	default:
		return errors.New("unknown connection type")
	}

	return nil
}
