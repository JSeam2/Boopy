/*
Networking implementation for boopy's gRPC protocol calls.
*/

package boopy

import (
	"google.golang.org/grpc"
	"github.com/Jseam2/Boopy/models"
	"time"
	"net"
)

func Dial(addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, opts...)
}

var {
	emptyNode				= &abstractions.Node{}
}

/*
Network interface allows nodes to talk to other nodes in the ring.
*/
type Network interface {
	Start() error
	Stop() error

	// RPC:
	// Successor
	GetSuccessor(*models.Node) (*models.Node, error)
	FindSuccessor(*models.Node, []byte) (*models.Node, error)
	SetSuccessor(*models.Node, *models.Node) error

	// Predecessor
	GetPredecessor(*models.Node) (*models.Node, error)
	CheckPredecessor(*models.Node) error
	SetPredecessor(*models.Node, *models.Node) error

	// Generics
	Notify(*models.Node, *models.Node) error
}

type GrpcTx struct {
	// Config
	config *Config

	timeout time.Duration
	maxIdle time.Duration

	// Socket
	sock *net.TCPListener

	// Pool and mutex of pool
	pool 	map[string]*grpcConn
	poolMux	sync.RWMutex
	
	server *grpc.Server

	shutdown int32
}

func NewGrpcTx(config *Config) (*GrpcTx, error){
	// Try starting the listener
	addr := config.Addr
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	pool := make(map[string]*grpcConn)

	// Setup Tx
	grp := &GrpcTx{
		sock: 		listener.(*net.TCPListener),
		timeout: 	config.Timeout,
		maxIdle: 	config.MaxIdle,
		pool:		pool,
		config:		config,
	}

	grp.server = grpc.NewServer(config.ServerOpts...)

	return grp, nil
}