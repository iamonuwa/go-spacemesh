package connectionpool

import (
	"github.com/spacemeshos/go-spacemesh/crypto"
	"github.com/spacemeshos/go-spacemesh/p2p/net"

	"errors"
	"gopkg.in/op/go-logging.v1"
	"sync"
)

type dialResult struct {
	conn net.Connection
	err  error
}

type networker interface {
	Dial(address string, remotePublicKey crypto.PublicKey) (net.Connection, error) // Connect to a remote node. Can send when no error.
	SubscribeOnNewRemoteConnections() chan net.Connection
	NetworkID() int8
	ClosingConnections() chan net.Connection
	Logger() *logging.Logger
}

// ConnectionPool stores all net.Connections and make them available to all users of net.Connection.
// There are two sources of connections -
// - Local connections that were created by local node (by calling GetConnection)
// - Remote connections that were provided by a networker impl. in a pub-sub manner
type ConnectionPool struct {
	localPub    crypto.PublicKey
	net         networker
	connections map[string]net.Connection
	connMutex   sync.RWMutex
	pending     map[string][]chan dialResult
	pendMutex   sync.Mutex
	dialWait    sync.WaitGroup
	shutdown    bool

	OnClose func(connection net.Connection)

	newRemoteConn chan net.Connection
	teardown      chan struct{}
}

// NewConnectionPool creates new ConnectionPool
func NewConnectionPool(network networker, lPub crypto.PublicKey) *ConnectionPool {
	connC := network.SubscribeOnNewRemoteConnections()
	cPool := &ConnectionPool{
		localPub:      lPub,
		net:           network,
		connections:   make(map[string]net.Connection),
		connMutex:     sync.RWMutex{},
		pending:       make(map[string][]chan dialResult),
		pendMutex:     sync.Mutex{},
		dialWait:      sync.WaitGroup{},
		shutdown:      false,
		newRemoteConn: connC,
		teardown:      make(chan struct{}),
	}
	go cPool.beginEventProcessing()
	return cPool
}

// Shutdown of the ConnectionPool, gracefully.
// - Close all open connections
// - Waits for all Dial routines to complete and unblock any routines waiting for GetConnection
func (cp *ConnectionPool) Shutdown() {
	cp.connMutex.Lock()
	if cp.shutdown {
		cp.connMutex.Unlock()
		cp.net.Logger().Error("shutdown was already called")
		return
	}
	cp.shutdown = true
	cp.connMutex.Unlock()

	cp.dialWait.Wait()
	cp.teardown <- struct{}{}
	// we won't handle the closing connection events for these connections since we exit the loop once the teardown is done
	cp.closeConnections()
}

func (cp *ConnectionPool) closeConnections() {
	cp.connMutex.Lock()
	// there should be no new connections arriving at this point
	for _, c := range cp.connections {
		c.Close()
	}
	cp.connMutex.Unlock()
}

func (cp *ConnectionPool) handleDialResult(rPub crypto.PublicKey, result dialResult) {
	cp.pendMutex.Lock()
	for _, p := range cp.pending[rPub.String()] {
		p <- result
	}
	delete(cp.pending, rPub.String())
	cp.pendMutex.Unlock()
}

func (cp *ConnectionPool) handleNewConnection(rPub crypto.PublicKey, conn net.Connection, source net.ConnectionSource) {
	cp.connMutex.Lock()
	cp.net.Logger().Debug("new connection %v -> %v. id=%s", cp.localPub, rPub, conn.ID())
	// check if there isn't already same connection (possible if the second connection is a Remote connection)
	//_, ok := cp.connections[rPub.String()]
	//if ok {
	//	cp.connMutex.Unlock()
	//	if source == net.Remote {
	//		cp.net.Logger().Info("connection created by remote node while connection already exists between peers, closing new connection. remote=%s", rPub)
	//	} else {
	//		cp.net.Logger().Warning("connection created by local node while connection already exists between peers, closing new connection. remote=%s", rPub)
	//	}
	//	return
	//} allways take new conntions
	cp.connections[rPub.String()] = conn
	cp.connMutex.Unlock()

	// update all registered channels
	res := dialResult{conn, nil}
	cp.handleDialResult(rPub, res)
}

func (cp *ConnectionPool) handleClosedConnection(conn net.Connection) {
	cp.net.Logger().Debug("connection %v with %v was closed", conn.String())
	cp.connMutex.Lock()
	rPub := conn.RemotePublicKey().String()
	cur, ok := cp.connections[rPub]
	// only delete if the closed connection is the same as the cached one (it is possible that the closed connection is a duplication and therefore was closed)
	if ok && cur.ID() == conn.ID() {
		delete(cp.connections, rPub)
	}
	cp.connMutex.Unlock()
	if cp.OnClose != nil {
		cp.OnClose(conn)
	}
}

// GetConnectionIfExists returns the connection if it exists, it will return nil and an err if not.
func (cp *ConnectionPool) GetConnectionIfExist(remotePub string) (net.Connection, error) {
	cp.connMutex.RLock()
	if cp.shutdown {
		cp.connMutex.RUnlock()
		return nil, errors.New("ConnectionPool was shut down")
	}
	// look for the connection in the pool
	conn, found := cp.connections[remotePub]
	if found {
		cp.connMutex.RUnlock()
		return conn, nil
	}
	return nil, errors.New("there is no connection with this key")
}

// GetConnection fetchs or creates if don't exist a connection to the address which is associated with the remote public key
func (cp *ConnectionPool) GetConnection(address string, remotePub crypto.PublicKey) (net.Connection, error) {
	cp.connMutex.RLock()
	if cp.shutdown {
		cp.connMutex.RUnlock()
		return nil, errors.New("ConnectionPool was shut down")
	}
	// look for the connection in the pool
	conn, found := cp.connections[remotePub.String()]
	if found {
		cp.connMutex.RUnlock()
		return conn, nil
	}
	// register for signal when connection is established - must be called under the connMutex otherwise there is a race
	// where it is possible that the connection will be established and all registered channels will be notified before
	// the current registration
	cp.pendMutex.Lock()
	_, found = cp.pending[remotePub.String()]
	if !found {
		// No one is waiting for a connection with the remote peer, need to call Dial
		go func() {
			// its annoying to check twice but we must to save some dials
			//cp.connMutex.RLock() TODO: Check why this doesn't work
			//if conn2, ok := cp.connections[remotePub.String()]; ok {
			//	cp.handleDialResult(remotePub, dialResult{conn2, nil})
			//}
			//cp.connMutex.RUnlock()
			//return
			cp.dialWait.Add(1)
			conn, err := cp.net.Dial(address, remotePub)
			if err != nil {
				cp.handleDialResult(remotePub, dialResult{nil, err})
			} else {
				cp.handleNewConnection(remotePub, conn, net.Local)
			}
			cp.dialWait.Done()
		}()
	}
	pendChan := make(chan dialResult)
	cp.pending[remotePub.String()] = append(cp.pending[remotePub.String()], pendChan)
	cp.pendMutex.Unlock()
	cp.connMutex.RUnlock()
	// wait for the connection to be established, if the channel is closed (in case of dialing error) will return nil
	res := <-pendChan
	return res.conn, res.err
}

func (cp *ConnectionPool) beginEventProcessing() {
Loop:
	for {
		select {
		case conn := <-cp.newRemoteConn:
			cp.handleNewConnection(conn.RemotePublicKey(), conn, net.Remote)

		case conn := <-cp.net.ClosingConnections():
			cp.handleClosedConnection(conn)

		case <-cp.teardown:
			break Loop
		}
	}
}
