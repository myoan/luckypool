package luckypool

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type server struct {
	dsn  string
	conn net.Conn
}

func newServer(dsn string) (*server, error) {
	addr, err := net.ResolveTCPAddr("tcp", dsn)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(addr.Network(), addr.String(), time.Minute)
	if err != nil {
		return nil, err
	}

	return &server{
		dsn:  dsn,
		conn: conn,
	}, nil
}

func (s *server) close() error {
	return s.conn.Close()
}

func parseValue(line string) (valueCmd, error) {
	elems := strings.Split(line, " ")
	if elems[0] != "VALUE" {
		return valueCmd{}, errors.New("result not VALUE")
	}
	if len(elems) != 4 {
		return valueCmd{}, errors.New("invalid length")
	}
	flag, err := strconv.Atoi(elems[2])
	if err != nil {
		return valueCmd{}, err
	}
	length, err := strconv.Atoi(elems[3][:len(elems[3])-2])
	if err != nil {
		return valueCmd{}, err
	}
	return valueCmd{
		key:    elems[1],
		flag:   flag,
		length: length,
	}, nil
}

type valueCmd struct {
	key    string
	flag   int
	length int
}

func (s *server) get(key string) ([]byte, error) {
	rw := bufio.NewReadWriter(bufio.NewReader(s.conn), bufio.NewWriter(s.conn))
	fmt.Fprintf(rw, "get %s\r\n", key)
	rw.Flush()

	line, err := rw.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	v, err := parseValue(string(line))
	if err != nil {
		return nil, err
	}

	data := make([]byte, v.length)
	_, err = rw.Read(data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *server) set(key string, value []byte) error {
	rw := bufio.NewReadWriter(bufio.NewReader(s.conn), bufio.NewWriter(s.conn))
	fmt.Fprintf(rw, "set %s 0 0 %d\r\n", key, len(value))
	rw.Write(value)
	rw.Write([]byte("\r\n"))
	rw.Flush()

	_, err := rw.ReadSlice('\n')
	if err != nil {
		return err
	}
	return nil
}

func (s *server) delete(key string) error {
	rw := bufio.NewReadWriter(bufio.NewReader(s.conn), bufio.NewWriter(s.conn))
	fmt.Fprintf(rw, "delete %s\r\n", key)
	rw.Flush()

	_, err := rw.ReadSlice('\n')
	if err != nil {
		return err
	}
	return nil

}

type Client struct {
	mx      sync.Mutex
	local   *server
	servers map[string]*server
}

func New(dsn string) (*Client, error) {
	local, err := newServer(dsn)
	if err != nil {
		return nil, err
	}
	return &Client{
		local:   local,
		servers: make(map[string]*server),
	}, nil
}

func (m *Client) each(f func(server *server) error) error {
	err := f(m.local)
	if err != nil {
		return err
	}

	for _, s := range m.servers {
		if err = f(s); err != nil {
			return err
		}
	}
	return nil
}

func (m *Client) Set(key string, value []byte) error {
	m.each(func(s *server) error {
		return s.set(key, value)
	})
	return nil
}

func (m *Client) Delete(key string) error {
	m.each(func(s *server) error {
		return s.delete(key)
	})
	return nil
}

func (m *Client) Get(key string) ([]byte, error) {
	data, err := m.local.get(key)
	if err != nil {
		return nil, err
	}
	return data, nil

}

func (m *Client) AddPools(dsns []string) error {
	m.mx.Lock()
	defer m.mx.Unlock()

	for _, dsn := range dsns {
		s, err := newServer(dsn)
		if err != nil {
			return err
		}
		m.servers[dsn] = s
	}

	return nil
}

func (m *Client) Close(dsn string) error {
	m.mx.Lock()
	defer m.mx.Unlock()

	s, ok := m.servers[dsn]
	if !ok {
		return errors.New("server not found")
	}
	err := s.close()
	if err != nil {
		return err
	}
	delete(m.servers, dsn)
	return nil
}

func (m *Client) CloseAll() error {
	m.mx.Lock()
	defer m.mx.Unlock()

	err := m.each(func(s *server) error {
		return s.close()
	})
	if err != nil {
		return err
	}

	m.servers = make(map[string]*server)
	return nil
}
