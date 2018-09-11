package p2p

import (
	"testing"
	"time"

	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/spacemeshos/go-spacemesh/p2p/config"
	"github.com/spacemeshos/go-spacemesh/p2p/dht"
	"github.com/spacemeshos/go-spacemesh/p2p/message"
	"github.com/spacemeshos/go-spacemesh/p2p/net"
	"github.com/spacemeshos/go-spacemesh/p2p/node"
	"github.com/spacemeshos/go-spacemesh/p2p/pb"
	"github.com/spacemeshos/go-spacemesh/timesync"
	"github.com/stretchr/testify/assert"
)

func p2pTestInstance(t testing.TB, config config.Config) *swarm {
	port, err := node.GetUnboundedPort()
	assert.NoError(t, err, "Error getting a port", err)
	config.TCPPort = port
	p, err := newSwarm(config, true, true)
	assert.NoError(t, err, "Error creating p2p stack, err: %v", err)
	assert.NotNil(t, p)
	return p
}

const exampleProtocol = "EX"
const examplePayload = "Example"

func TestNew(t *testing.T) {
	s, err := New(config.DefaultConfig())
	assert.NoError(t, err, err)
	err = s.Start()
	assert.NoError(t, err, err)
	assert.NotNil(t, s, "its nil")
	s.Shutdown()
}

func Test_newSwarm(t *testing.T) {
	s, err := newSwarm(config.DefaultConfig(), true, false)
	assert.NoError(t, err)
	err = s.Start()
	assert.NoError(t, err, err)
	assert.NotNil(t, s)
	s.Shutdown()
}

func TestSwarm_Shutdown(t *testing.T) {
	s, err := newSwarm(config.DefaultConfig(), true, false)
	assert.NoError(t, err)
	err = s.Start()
	assert.NoError(t, err, err)
	s.Shutdown()

	select {
	case _, ok := <-s.shutdown:
		assert.False(t, ok)
	case <-time.After(1 * time.Second):
		t.Error("Failed to shutdown")
	}
}

func TestSwarm_ShutdownNoStart(t *testing.T) {
	s, err := newSwarm(config.DefaultConfig(), true, false)
	assert.NoError(t, err)
	s.Shutdown()
}

func TestSwarm_RegisterProtocolNoStart(t *testing.T) {
	s, err := newSwarm(config.DefaultConfig(), true, false)
	msgs := s.RegisterProtocol("Anton")
	assert.NotNil(t, msgs)
	assert.NoError(t, err)
	s.Shutdown()
}

func TestSwarm_processMessage(t *testing.T) {
	s := swarm{}
	s.lNode, _ = node.GenerateTestNode(t)
	r := node.GenerateRandomNodeData()
	c := &net.ConnectionMock{}
	c.SetRemotePublicKey(r.PublicKey())
	ime := net.IncomingMessageEvent{Message: nil, Conn: c}
	s.processMessage(ime) // should error

	assert.True(t, c.Closed())
}

func TestSwarm_updateConnection(t *testing.T) {
	p := &swarm{}
	p.lNode, _ = node.GenerateTestNode(t)
	dhtmock := &dht.MockDHT{}
	p.dht = dhtmock
	c := &net.ConnectionMock{}

	p.updateConnection(c)

	assert.Equal(t, dhtmock.UpdateCount(), 0)

	r := node.GenerateRandomNodeData()
	c.SetRemotePublicKey(r.PublicKey())

	p.updateConnection(c)

	assert.Equal(t, dhtmock.UpdateCount(), 1)
}

func TestSwarm_RoundTrip(t *testing.T) {
	t.Skip()
	p1 := p2pTestInstance(t, config.DefaultConfig())
	p2 := p2pTestInstance(t, config.DefaultConfig())

	exchan := p1.RegisterProtocol(exampleProtocol)

	assert.Equal(t, exchan, p1.protocolHandlers[exampleProtocol])

	err := p2.SendMessage(p1.lNode.String(), exampleProtocol, []byte(examplePayload))

	assert.Error(t, err, "ERR") // should'nt be in routing table

	p2.dht.Update(p1.lNode.Node)

	err = p2.SendMessage(p1.lNode.String(), exampleProtocol, []byte(examplePayload))

	select {
	case msg := <-exchan:
		assert.Equal(t, msg.Data(), []byte(examplePayload))

		assert.Equal(t, msg.Sender().String(), p2.lNode.String())
		break
	case <-time.After(1 * time.Second):
		t.Error("Took to much time to recieve")
	}

}

func TestSwarm_RegisterProtocol(t *testing.T) {
	const numPeers = 100
	nodechan := make(chan *swarm)
	cfg := config.DefaultConfig()
	for i := 0; i < numPeers; i++ {
		go func() { // protocols are registered before starting the node and read after that.
			// there ins't an actual need to sync them.
			nod := p2pTestInstance(t, cfg)
			nod.RegisterProtocol(exampleProtocol) // this is example
			nodechan <- nod
		}()
	}
	i := 0
	for r := range nodechan {
		_, ok := r.protocolHandlers[exampleProtocol]
		assert.True(t, ok)
		_, ok = r.protocolHandlers["/dht/1.0/find-node/"]
		assert.True(t, ok)
		i++
		if i == numPeers {
			close(nodechan)
		}
	}
}

func TestSwarm_onRemoteClientMessage(t *testing.T) {
	cfg := config.DefaultConfig()
	id, err := node.NewNodeIdentity(cfg, "0.0.0.0:0000", false)
	assert.NoError(t, err, "we cant make node ?")

	p := p2pTestInstance(t, config.DefaultConfig())
	nmock := &net.ConnectionMock{}
	nmock.SetRemotePublicKey(id.PublicKey())

	// Test bad format

	msg := []byte("badbadformat")
	imc := net.IncomingMessageEvent{nmock, msg}
	err = p.onRemoteClientMessage(imc)
	assert.Equal(t, err, ErrBadFormat1)

	// Test out of sync

	realmsg := &pb.CommonMessageData{
		SessionId: []byte("test"),
		Payload:   []byte("test"),
		Timestamp: time.Now().Add(timesync.MaxAllowedMessageDrift + time.Minute).Unix(),
	}
	bin, _ := proto.Marshal(realmsg)

	imc.Message = bin

	err = p.onRemoteClientMessage(imc)
	assert.Equal(t, err, ErrOutOfSync)

	// Test no payload

	cmd := &pb.CommonMessageData{
		SessionId: []byte("test"),
		Payload:   []byte(""),
		Timestamp: time.Now().Unix(),
	}

	bin, _ = proto.Marshal(cmd)
	imc.Message = bin
	err = p.onRemoteClientMessage(imc)

	assert.Equal(t, err, ErrNoPayload)

	// Test No Session

	cmd.Payload = []byte("test")

	bin, _ = proto.Marshal(cmd)
	imc.Message = bin

	err = p.onRemoteClientMessage(imc)
	assert.Equal(t, err, ErrNoSession)

	// Test bad session

	session := &net.SessionMock{}
	session.SetDecrypt(nil, errors.New("fail"))
	imc.Conn.SetSession(session)

	err = p.onRemoteClientMessage(imc)
	assert.Equal(t, err, ErrFailDecrypt)

	// Test bad format again

	session.SetDecrypt([]byte("wont_format_fo_protocol_message"), nil)

	err = p.onRemoteClientMessage(imc)
	assert.Equal(t, err, ErrBadFormat2)

	// Test bad auth sign

	goodmsg := &pb.ProtocolMessage{
		Metadata: message.NewProtocolMessageMetadata(id.PublicKey(), exampleProtocol, false), // not signed
		Payload:  []byte(examplePayload),
	}

	goodbin, _ := proto.Marshal(goodmsg)

	cmd.Payload = goodbin
	bin, _ = proto.Marshal(cmd)
	imc.Message = bin
	session.SetDecrypt(goodbin, nil)

	err = p.onRemoteClientMessage(imc)
	assert.Equal(t, err, ErrAuthAuthor)

	// Test no protocol

	err = message.SignMessage(id.PrivateKey(), goodmsg)
	assert.NoError(t, err, err)

	goodbin, _ = proto.Marshal(goodmsg)
	cmd.Payload = goodbin
	bin, _ = proto.Marshal(cmd)
	imc.Message = bin
	session.SetDecrypt(goodbin, nil)

	err = p.onRemoteClientMessage(imc)
	assert.Equal(t, err, ErrNoProtocol)

	// Test no err

	c := p.RegisterProtocol(exampleProtocol)
	go func() { <-c }()

	err = p.onRemoteClientMessage(imc)

	assert.NoError(t, err)

	// todo : test gossip codepaths.
}
