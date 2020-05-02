package boopy

import "time"

// Periodically fix finger tables.
// periodically runs down finger table, recreating finger entries for each finger table ID
func (node *Node) fixFingerRoutine(val int) {
	next := 0
	timer := time.NewTicker(time.Duration(val) * time.Millisecond)
	for {
		select {
		case <-timer.C:
			// found in finger.go,
			next = node.fixFinger(next)
		case <-node.shutdownCh:
			timer.Stop()
			return
		}
	}
}

// Stabilize Routine
func (node *Node) stabilizeRoutine(val int) {
	timer := time.NewTicker(time.Duration(val) * time.Millisecond)
	for {
		select {
		case <-timer.C:
			node.stabilize()
		case <-node.shutdownCh:
			timer.Stop()
			return
		}
	}

}

// Check predecessor failed routine
func (node *Node) checkPredecessorRoutine(val int) {
	ticker := time.NewTicker(time.Duration(val) * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			node.checkPredecessor()
		case <-node.shutdownCh:
			ticker.Stop()
			return
		}
	}

}
