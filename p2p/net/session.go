package net

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"github.com/spacemeshos/go-spacemesh/log"
	"sync"
	"time"
)

// NetworkSession is an authenticated network session between 2 peers.
// Sessions may be used between 'connections' until they expire.
// Session provides the encryptor/decryptor for all messages exchanged between 2 peers.
// enc/dec is using an ephemeral sym key exchanged securely between the peers via the handshake protocol
// The handshake protocol goal is to create an authenticated network session.
type NetworkSession interface {
	ID() []byte     // Unique session id
	KeyM() []byte   // session shared sym key for mac - 32 bytes
	PubKey() []byte // 65 bytes session-only pub key uncompressed

	Decrypt(in []byte) ([]byte, error) // decrypt data using session dec key
	Encrypt(in []byte) ([]byte, error) // encrypt data using session enc key
}

// TODO: add support for idle session expiration

// NetworkSessionImpl implements NetworkSession.
type NetworkSessionImpl struct {
	id      []byte
	keyE    []byte
	keyM    []byte
	pubKey  []byte
	created time.Time

	// We must protect authenticated somehow. we won't use event loop for one state variable
	authMutex     sync.RWMutex
	authenticated bool

	localNodeID  string
	remoteNodeID string

	crypterLock    sync.Mutex
	blockEncrypter cbcMode
	blockDecrypter cbcMode
}

// cbcMode is an interface for block ciphers using cipher block chaining.
// we use it to expose SetIV
type cbcMode interface {
	cipher.BlockMode
	SetIV([]byte)
}

// resetIV sets the iv to the session ID. it is called after encrpyt decrypt.
func (n *NetworkSessionImpl) resetIV() {
	n.blockEncrypter.SetIV(n.id)
	n.blockDecrypter.SetIV(n.id)
}

//LocalNodeID returns the session's local node id.
func (n *NetworkSessionImpl) LocalNodeID() string {
	return n.localNodeID
}

//RemoteNodeID returns the session's remote node id.
func (n *NetworkSessionImpl) RemoteNodeID() string {
	return n.remoteNodeID
}

// String returns the session's identifier string.
func (n *NetworkSessionImpl) String() string {
	return hex.EncodeToString(n.id)
}

// ID returns the session's unique id
func (n *NetworkSessionImpl) ID() []byte {
	return n.id
}

// KeyE returns the sessions sym encryption key.
func (n *NetworkSessionImpl) KeyE() []byte {
	return n.keyE
}

// KeyM returns the session's MAC encryption key.
func (n *NetworkSessionImpl) KeyM() []byte {
	return n.keyM
}

// PubKey returns the session's public key.
func (n *NetworkSessionImpl) PubKey() []byte {
	return n.pubKey
}

// Created returns the session creation time.
func (n *NetworkSessionImpl) Created() time.Time {
	return n.created
}

// Encrypt encrypts in binary data with the session's sym enc key.
func (n *NetworkSessionImpl) Encrypt(in []byte) ([]byte, error) {
	//l := len(in)
	//if l == 0 {
	//	return nil, errors.New("Invalid input buffer - 0 len")
	//}
	//paddedIn := crypto.AddPKCSPadding(in)
	//out := make([]byte, len(paddedIn))
	//n.crypterLock.Lock()
	//n.blockEncrypter.CryptBlocks(out, paddedIn)
	//n.crypterLock.Unlock()
	//n.resetIV()
	return in, nil
}

// Decrypt decrypts in binary data that was encrypted with the session's sym enc key.
func (n *NetworkSessionImpl) Decrypt(in []byte) ([]byte, error) {
	//l := len(in)
	//if l == 0 {
	//	return nil, errors.New("Invalid input buffer - 0 len")
	//}
	//
	//n.crypterLock.Lock()
	//n.blockDecrypter.CryptBlocks(in, in)
	//n.crypterLock.Unlock()
	//clearText, err := crypto.RemovePKCSPadding(in)
	//if err != nil {
	//	return nil, err
	//}
	//n.resetIV()
	return in, nil
}

// NewNetworkSession creates a new network session based on provided data
func NewNetworkSession(id, keyE, keyM, pubKey []byte, localNodeID, remoteNodeID string) (*NetworkSessionImpl, error) {
	n := &NetworkSessionImpl{
		id:            id,
		keyE:          keyE,
		keyM:          keyM,
		pubKey:        pubKey,
		created:       time.Now(),
		authMutex:     sync.RWMutex{},
		authenticated: false,
		localNodeID:   localNodeID,
		remoteNodeID:  remoteNodeID,
	}

	// create and store block enc/dec
	blockCipher, err := aes.NewCipher(keyE)
	if err != nil {
		log.Error("Failed to create block cipher")
		return nil, err
	}

	n.blockEncrypter = cipher.NewCBCEncrypter(blockCipher, n.id).(cbcMode)
	n.blockDecrypter = cipher.NewCBCDecrypter(blockCipher, n.id).(cbcMode)

	return n, nil
}
