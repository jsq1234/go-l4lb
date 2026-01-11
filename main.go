package main

import (
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"syscall"
)

const (
	POOL_SIZE    = 10000 // Connections per backend
	BUFFER_SIZE  = 32 * 1024
	WORKER_COUNT = 10000
)

type Backend struct {
	address  string
	active   int32
	healthy  int32
	connPool chan net.Conn
}

func NewBackend(address string) *Backend {
	b := &Backend{
		address:  address,
		active:   0,
		healthy:  1,
		connPool: make(chan net.Conn, POOL_SIZE),
	}
	go b.healthChecker()
	return b
}

func isConnClosed(c net.Conn) bool {
    tc, ok := c.(*net.TCPConn)
    if !ok {
        return false 
    }

    rawConn, err := tc.SyscallConn()
    if err != nil {
        return true
    }

    var sysErr error
    var n int

    err = rawConn.Read(func(fd uintptr) bool {
        var buf [1]byte
        n, _, sysErr = syscall.Recvfrom(int(fd), buf[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
        return true
    })

    if sysErr == syscall.EAGAIN || sysErr == syscall.EWOULDBLOCK {
        return false
    }

    if n == 0 && sysErr == nil {
        return true
    }
    if sysErr != nil {
        return true
    }
    return false
}

func (b *Backend) getConn() (net.Conn, error) {
	// Try to get from pool first
	for{
		select {
		case conn := <-b.connPool:
			if isConnClosed(conn) {
				conn.Close()
				continue
			}
			return conn, nil
		default:
			// Create new if none in pool
			conn, err := net.DialTimeout("tcp", b.address, 3*time.Second)
			if err != nil {
				return nil, err
			}
	
			if tc, ok := conn.(*net.TCPConn); ok {
				tc.SetNoDelay(true)
				tc.SetKeepAlive(true)
				tc.SetKeepAlivePeriod(30 * time.Second)
				tc.SetReadBuffer(BUFFER_SIZE)
				tc.SetWriteBuffer(BUFFER_SIZE)
			}
	
			return conn, nil
		}
	}
}

func (b *Backend) releaseConn(conn net.Conn) {
	select {
	case b.connPool <- conn:
		// Returned to pool
	default:
		// Pool full, close
		conn.Close()
	}
}

func (b *Backend) healthChecker() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		conn, err := net.DialTimeout("tcp", b.address, 2*time.Second)
		if err != nil {
			atomic.StoreInt32(&b.healthy, 0)
			continue
		}
		conn.Close()
		atomic.StoreInt32(&b.healthy, 1)
	}
}

// Buffer pool to reduce allocations
var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, BUFFER_SIZE)
	},
}

func main() {
	backends := []*Backend{
		NewBackend("localhost:8080"),
		NewBackend("localhost:8081"),
		NewBackend("localhost:8082"),
		NewBackend("localhost:8083"),
	}

	l, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	log.Println("Load balancer started on :9090")

	// Connection semaphore
	maxConn := make(chan struct{}, 10000)

	// Main accept loop
	for {
		maxConn <- struct{}{} // Block if too many connections

		conn, err := l.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			<-maxConn
			continue
		}

		go func(client net.Conn) {
			defer func() {
				client.Close()
				<-maxConn
			}()

			// Set client timeouts
			client.SetDeadline(time.Now().Add(30 * time.Second))

			// Pick least loaded healthy backend
			var selectedBackend *Backend
			minActive := int32(1<<31 - 1)

			for _, b := range backends {
				if atomic.LoadInt32(&b.healthy) == 1 {
					active := atomic.LoadInt32(&b.active)
					if active < minActive {
						selectedBackend = b
						minActive = active
					}
				}
			}

			if selectedBackend == nil {
				log.Println("No healthy backends available")
				return
			}

			// Get backend connection
			backend, err := selectedBackend.getConn()
			if err != nil {
				log.Printf("Backend connection error: %v", err)
				return
			}

			// Track active connection
			atomic.AddInt32(&selectedBackend.active, 1)
			defer atomic.AddInt32(&selectedBackend.active, -1)

			// Set backend timeouts
			backend.SetDeadline(time.Now().Add(30 * time.Second))

			// Create WaitGroup for the two copy operations
			var wg sync.WaitGroup
			wg.Add(2)

			// Client to backend
			go func() {
				defer wg.Done()
				buf := bufferPool.Get().([]byte)
				defer bufferPool.Put(buf)

				_, err := io.CopyBuffer(backend, client, buf)
				if err != nil && !isConnectionClosed(err) {
					log.Printf("Error copying client->backend: %v", err)
				}

				// Signal EOF to backend
				if tc, ok := backend.(*net.TCPConn); ok {
					tc.CloseWrite()
				}
			}()

			// Backend to client
			go func() {
				defer wg.Done()
				buf := bufferPool.Get().([]byte)
				defer bufferPool.Put(buf)

				_, err := io.CopyBuffer(client, backend, buf)
				if err != nil && !isConnectionClosed(err) {
					log.Printf("Error copying backend->client: %v", err)
				}

				// Signal EOF to client
				if tc, ok := client.(*net.TCPConn); ok {
					tc.CloseWrite()
				}
			}()

			// Wait for both directions to complete
			wg.Wait()

			// Try to return backend connection to pool
			selectedBackend.releaseConn(backend)

		}(conn)
	}
}

// Helper to check if error is just a closed connection
func isConnectionClosed(err error) bool {
	if err == io.EOF {
		return true
	}
	if opErr, ok := err.(*net.OpError); ok {
		return opErr.Err.Error() == "use of closed network connection"
	}
	return false
}
