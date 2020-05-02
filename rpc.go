package boopy

import (
	"fmt"

	"github.com/jseam2/boopy/api"
	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////
// RPC Call Functions
////////////////////////////////////////////////////////////////

// getSuccessorRPC the successor ID of a remote node.
func (n *Node) getSuccessorRPC(node *api.Node) (*api.Node, error) {
	return n.transport.GetSuccessor(node)
}

// setSuccessorRPC sets the successor of a given node.
func (n *Node) setSuccessorRPC(node *api.Node, succ *api.Node) error {
	return n.transport.SetSuccessor(node, succ)
}

// findSuccessorRPC finds the successor node of a given ID in the entire ring.
func (n *Node) findSuccessorRPC(node *api.Node, id []byte) (*api.Node, error) {
	return n.transport.FindSuccessor(node, id)
}

// getSuccessorRPC the successor ID of a remote node.
func (n *Node) getPredecessorRPC(node *api.Node) (*api.Node, error) {
	return n.transport.GetPredecessor(node)
}

// setPredecessorRPC sets the predecessor of a given node.
func (n *Node) setPredecessorRPC(node *api.Node, pred *api.Node) error {
	return n.transport.SetPredecessor(node, pred)
}

// notify notifies a remote node that pred is its predecessor.
func (n *Node) notify(node, pred *api.Node) error {
	return n.transport.Notify(node, pred)
}

func (n *Node) getKeyRPC(node *api.Node, key string) (*api.GetResponse, error) {
	return n.transport.GetKey(node, key)
}
func (n *Node) setKeyRPC(node *api.Node, key, value string) error {
	return n.transport.SetKey(node, key, value)
}
func (n *Node) deleteKeyRPC(node *api.Node, key string) error {
	return n.transport.DeleteKey(node, key)
}

func (n *Node) requestKeysRPC(
	node *api.Node, from []byte, to []byte,
) ([]*api.KV, error) {
	return n.transport.RequestKeys(node, from, to)
}

func (n *Node) deleteKeysRPC(
	node *api.Node, keys []string,
) error {
	return n.transport.DeleteKeys(node, keys)
}

////////////////////////////////////////////////////////////////
// RPC Interface Implementation
////////////////////////////////////////////////////////////////

func (n *Node) GetSuccessor(ctx context.Context, r *api.ER) (*api.Node, error) {
	n.succMtx.RLock()
	succ := n.successor
	n.succMtx.RUnlock()
	if succ == nil {
		return emptyNode, nil
	}

	return succ, nil
}

func (n *Node) SetSuccessor(ctx context.Context, succ *api.Node) (*api.ER, error) {
	n.succMtx.Lock()
	n.successor = succ
	n.succMtx.Unlock()
	return emptyRequest, nil
}

func (n *Node) FindSuccessor(ctx context.Context, id *api.ID) (*api.Node, error) {
	succ, err := n.findSuccessor(id.Id)
	// If there's an error
	if err != nil {
		return nil, err
	}

	// If we can't find our successor
	if succ == nil {
		return nil, ERR_NO_SUCCESSOR
	}

	return succ, nil

}

func (n *Node) CheckPredecessor(ctx context.Context, id *api.ID) (*api.ER, error) {
	return emptyRequest, nil
}

func (n *Node) GetPredecessor(ctx context.Context, r *api.ER) (*api.Node, error) {
	n.predMtx.RLock()
	pred := n.predecessor
	n.predMtx.RUnlock()
	if pred == nil {
		return emptyNode, nil
	}
	return pred, nil
}

func (n *Node) SetPredecessor(ctx context.Context, pred *api.Node) (*api.ER, error) {
	n.predMtx.Lock()
	n.predecessor = pred
	n.predMtx.Unlock()
	return emptyRequest, nil
}

func (n *Node) Notify(ctx context.Context, node *api.Node) (*api.ER, error) {
	n.predMtx.Lock()
	defer n.predMtx.Unlock()
	var prevPredNode *api.Node

	pred := n.predecessor
	if pred == nil || between(node.Id, pred.Id, n.Id) {
		if n.predecessor != nil {
			prevPredNode = n.predecessor
		}
		n.predecessor = node

		if prevPredNode != nil {
			if between(n.predecessor.Id, prevPredNode.Id, n.Id) {
				n.transferKeys(prevPredNode, n.predecessor)
			}
		}

	}

	return emptyRequest, nil
}

func (n *Node) XGet(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	n.stMtx.RLock()
	defer n.stMtx.RUnlock()
	val, err := n.storage.Get(req.Key)
	if err != nil {
		return emptyGetResponse, err
	}
	return &api.GetResponse{Value: val}, nil
}

func (n *Node) XSet(ctx context.Context, req *api.SetRequest) (*api.SetResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	fmt.Println("setting key on ", n.Node.Addr, req.Key, req.Value)
	err := n.storage.Set(req.Key, req.Value)
	return emptySetResponse, err
}

func (n *Node) XDelete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	err := n.storage.Delete(req.Key)
	return emptyDeleteResponse, err
}

func (n *Node) XRequestKeys(ctx context.Context, req *api.RequestKeysRequest) (*api.RequestKeysResponse, error) {
	n.stMtx.RLock()
	defer n.stMtx.RUnlock()
	val, err := n.storage.Between(req.From, req.To)
	if err != nil {
		return emptyRequestKeysResponse, err
	}
	return &api.RequestKeysResponse{Values: val}, nil
}

func (n *Node) XMultiDelete(ctx context.Context, req *api.MultiDeleteRequest) (*api.DeleteResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	err := n.storage.MDelete(req.Keys...)
	return emptyDeleteResponse, err
}
