package boopy

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jseam2/boopy/api"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	emptyNode                = &api.Node{}
	emptyRequest             = &api.ER{}
	emptyGetResponse         = &api.GetResponse{}
	emptySetResponse         = &api.SetResponse{}
	emptyDeleteResponse      = &api.DeleteResponse{}
	emptyRequestKeysResponse = &api.RequestKeysResponse{}
)

func Dial(addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, opts...)
}

/*
	Transport enables a node to talk to the other nodes in
	the ring
*/
type Transport interface {
	Start() error
	Stop() error

	//RPC
	GetSuccessor(*api.Node) (*api.Node, error)
	FindSuccessor(*api.Node, []byte) (*api.Node, error)
	SetSuccessor(*api.Node, *api.Node) error
	GetPredecessor(*api.Node) (*api.Node, error)
	CheckPredecessor(*api.Node) error
	SetPredecessor(*api.Node, *api.Node) error
	Notify(*api.Node, *api.Node) error

	//Storage
	GetKey(*api.Node, string) (*api.GetResponse, error)
	SetKey(*api.Node, string, string) error
	DeleteKey(*api.Node, string) error
	RequestKeys(*api.Node, []byte, []byte) ([]*api.KV, error)
	DeleteKeys(*api.Node, []string) error
}

type GrpcTransport struct {
	config *Config

	timeout time.Duration
	maxIdle time.Duration

	sock *net.TCPListener

	pool    map[string]*grpcConn
	poolMtx sync.RWMutex

	server *grpc.Server

	shutdown int32
}

// func NewGrpcTransport(config *Config) (api.ChordClient, error) {
func NewGrpcTransport(config *Config) (*GrpcTransport, error) {

	uriAddr := config.Addr
	// Try to start the tcpListener
	tcpListener, err := net.Listen("tcp", uriAddr)
	if err != nil {
		return nil, err
	}

	pool := make(map[string]*grpcConn)

	// Setup the transport
	grp := &GrpcTransport{
		sock:    tcpListener.(*net.TCPListener),
		timeout: config.MaxTimeoutDuration,
		maxIdle: config.MaxIdleDuration,
		pool:    pool,
		config:  config,
	}

	grp.server = grpc.NewServer(config.ServerOpts...)

	// Done
	return grp, nil
}

type grpcConn struct {
	addr       string
	client     api.ChordClient
	conn       *grpc.ClientConn
	lastActive time.Time
}

func (g *grpcConn) Close() {
	g.conn.Close()
}

func (gt *GrpcTransport) registerNode(node *Node) {
	api.RegisterChordServer(gt.server, node)
}

func (gt *GrpcTransport) GetServer() *grpc.Server {
	return gt.server
}

// Gets an outbound connection to a host
func (gt *GrpcTransport) getConn(
	addr string,
) (api.ChordClient, error) {

	gt.poolMtx.RLock()

	if atomic.LoadInt32(&gt.shutdown) == 1 {
		gt.poolMtx.Unlock()
		return nil, fmt.Errorf("TCP transport is shutdown")
	}

	cc, ok := gt.pool[addr]
	gt.poolMtx.RUnlock()
	if ok {
		return cc.client, nil
	}

	var conn *grpc.ClientConn
	var err error
	conn, err = Dial(addr, gt.config.DialOpts...)
	if err != nil {
		return nil, err
	}

	client := api.NewChordClient(conn)
	cc = &grpcConn{addr, client, conn, time.Now()}
	gt.poolMtx.Lock()
	if gt.pool != nil {
		gt.pool[addr] = cc
		gt.poolMtx.Unlock()
	} else {
		gt.poolMtx.Unlock()
		return nil, errors.New("must instantiate node before using")
	}

	return client, nil
}

func (gt *GrpcTransport) Start() error {
	// Start RPC server
	go gt.listen()

	// Reap old connections
	go gt.reapOld()
	return nil
}

// Returns an outbound TCP connection to the pool
func (gt *GrpcTransport) returnConn(o *grpcConn) {
	// Update the last asctive time
	o.lastActive = time.Now()

	// Push back into the pool
	gt.poolMtx.Lock()
	defer gt.poolMtx.Unlock()
	if atomic.LoadInt32(&gt.shutdown) == 1 {
		o.conn.Close()
		return
	}
	gt.pool[o.addr] = o
}

// Shutdown the TCP transport
func (gt *GrpcTransport) Stop() error {
	atomic.StoreInt32(&gt.shutdown, 1)

	// Close all the connections
	gt.server.Stop()
	gt.poolMtx.Lock()
	for _, conn := range gt.pool {
		conn.Close()
	}
	gt.pool = nil
	gt.poolMtx.Unlock()

	return nil
}

// Closes old outbound connections
func (gt *GrpcTransport) reapOld() {
	ticker := time.NewTicker(60 * time.Second)

	for {
		if atomic.LoadInt32(&gt.shutdown) == 1 {
			return
		}
		select {
		case <-ticker.C:
			gt.reap()
		}

	}
}

func (gt *GrpcTransport) reap() {
	gt.poolMtx.Lock()
	defer gt.poolMtx.Unlock()
	for host, conn := range gt.pool {
		if time.Since(conn.lastActive) > gt.maxIdle {
			conn.Close()
			delete(gt.pool, host)
		}
	}
}

// Listens for inbound connections
func (gt *GrpcTransport) listen() {
	gt.server.Serve(gt.sock)
}

// GetSuccessor the successor ID of a remote node.
func (gt *GrpcTransport) GetSuccessor(node *api.Node) (*api.Node, error) {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	conntx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	return client.GetSuccessor(conntx, emptyRequest)
}

// FindSuccessor the successor ID of a remote node.
func (gt *GrpcTransport) FindSuccessor(node *api.Node, id []byte) (*api.Node, error) {
	// fmt.Println("yo", node.Id, id)
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	conntx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	return client.FindSuccessor(conntx, &api.ID{Id: id})
}

// GetPredecessor the successor ID of a remote node.
func (gt *GrpcTransport) GetPredecessor(node *api.Node) (*api.Node, error) {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	conntx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	return client.GetPredecessor(conntx, emptyRequest)
}

func (gt *GrpcTransport) SetPredecessor(node *api.Node, predecessor *api.Node) error {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return err
	}

	conntx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	_, err = client.SetPredecessor(conntx, predecessor)
	return err
}

func (gt *GrpcTransport) SetSuccessor(node *api.Node, succ *api.Node) error {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return err
	}

	conntx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	_, err = client.SetSuccessor(conntx, succ)
	return err
}

func (gt *GrpcTransport) Notify(node, predecessor *api.Node) error {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return err
	}

	conntx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	_, err = client.Notify(conntx, predecessor)
	return err

}

func (gt *GrpcTransport) CheckPredecessor(node *api.Node) error {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return err
	}

	conntx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	_, err = client.CheckPredecessor(conntx, &api.ID{Id: node.Id})
	return err
}

func (gt *GrpcTransport) GetKey(node *api.Node, key string) (*api.GetResponse, error) {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	conntx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	return client.XGet(conntx, &api.GetRequest{Key: key})
}

func (gt *GrpcTransport) SetKey(node *api.Node, key, value string) error {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return err
	}

	conntx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	_, err = client.XSet(conntx, &api.SetRequest{Key: key, Value: value})
	return err
}

func (gt *GrpcTransport) DeleteKey(node *api.Node, key string) error {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return err
	}

	conntx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	_, err = client.XDelete(conntx, &api.DeleteRequest{Key: key})
	return err
}

func (gt *GrpcTransport) RequestKeys(node *api.Node, from, to []byte) ([]*api.KV, error) {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	conntx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	val, err := client.XRequestKeys(
		conntx, &api.RequestKeysRequest{From: from, To: to},
	)
	if err != nil {
		return nil, err
	}
	return val.Values, nil
}

func (gt *GrpcTransport) DeleteKeys(node *api.Node, keys []string) error {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return err
	}

	conntx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	_, err = client.XMultiDelete(
		conntx, &api.MultiDeleteRequest{Keys: keys},
	)
	return err
}
