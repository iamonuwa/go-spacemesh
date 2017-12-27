// Code generated by protoc-gen-go. DO NOT EDIT.
// source: node.proto

/*
Package pb is a generated protocol buffer package.

It is generated from these files:
	node.proto

It has these top-level messages:
	Node
*/
package pb

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// node type
type NodeType int32

const (
	NodeType_branch    NodeType = 0
	NodeType_extension NodeType = 1
	NodeType_leaf      NodeType = 2
)

var NodeType_name = map[int32]string{
	0: "branch",
	1: "extension",
	2: "leaf",
}
var NodeType_value = map[string]int32{
	"branch":    0,
	"extension": 1,
	"leaf":      2,
}

func (x NodeType) String() string {
	return proto.EnumName(NodeType_name, int32(x))
}
func (NodeType) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

type Node struct {
	NodeType NodeType `protobuf:"varint,1,opt,name=nodeType,enum=pb.NodeType" json:"nodeType,omitempty"`
	Entries  [][]byte `protobuf:"bytes,2,rep,name=entries,proto3" json:"entries,omitempty"`
	Parity   bool     `protobuf:"varint,3,opt,name=parity" json:"parity,omitempty"`
	Path     []byte   `protobuf:"bytes,4,opt,name=path,proto3" json:"path,omitempty"`
	Value    []byte   `protobuf:"bytes,5,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *Node) Reset()                    { *m = Node{} }
func (m *Node) String() string            { return proto.CompactTextString(m) }
func (*Node) ProtoMessage()               {}
func (*Node) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Node) GetNodeType() NodeType {
	if m != nil {
		return m.NodeType
	}
	return NodeType_branch
}

func (m *Node) GetEntries() [][]byte {
	if m != nil {
		return m.Entries
	}
	return nil
}

func (m *Node) GetParity() bool {
	if m != nil {
		return m.Parity
	}
	return false
}

func (m *Node) GetPath() []byte {
	if m != nil {
		return m.Path
	}
	return nil
}

func (m *Node) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func init() {
	proto.RegisterType((*Node)(nil), "pb.Node")
	proto.RegisterEnum("pb.NodeType", NodeType_name, NodeType_value)
}

func init() { proto.RegisterFile("node.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 195 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x3c, 0x8f, 0x41, 0x4a, 0xc6, 0x30,
	0x10, 0x46, 0x4d, 0xfe, 0xb4, 0xc6, 0xa1, 0x4a, 0x19, 0x44, 0xb2, 0x0c, 0xae, 0x82, 0x8b, 0x0a,
	0x7a, 0x03, 0x0f, 0xd0, 0x45, 0x70, 0xe5, 0x2e, 0xb1, 0x23, 0x2d, 0x94, 0x24, 0xa4, 0x51, 0xec,
	0x1d, 0x3c, 0xb4, 0x58, 0xdb, 0x7f, 0x37, 0xef, 0xbd, 0xc5, 0xf0, 0x01, 0x84, 0x38, 0x50, 0x97,
	0x72, 0x2c, 0x11, 0x79, 0xf2, 0xf7, 0x3f, 0x0c, 0x44, 0x1f, 0x07, 0x42, 0x03, 0xf2, 0x2f, 0xbd,
	0xae, 0x89, 0x14, 0xd3, 0xcc, 0xdc, 0x3c, 0x35, 0x5d, 0xf2, 0x5d, 0xbf, 0x3b, 0x7b, 0xae, 0xa8,
	0xe0, 0x92, 0x42, 0xc9, 0x13, 0x2d, 0x8a, 0xeb, 0x93, 0x69, 0xec, 0x81, 0x78, 0x07, 0x75, 0x72,
	0x79, 0x2a, 0xab, 0x3a, 0x69, 0x66, 0xa4, 0xdd, 0x09, 0x11, 0x44, 0x72, 0x65, 0x54, 0x42, 0x33,
	0xd3, 0xd8, 0xed, 0xc6, 0x5b, 0xa8, 0xbe, 0xdc, 0xfc, 0x49, 0xaa, 0xda, 0xe4, 0x3f, 0x3c, 0x3c,
	0x82, 0x3c, 0x3e, 0x22, 0x40, 0xed, 0xb3, 0x0b, 0xef, 0x63, 0x7b, 0x81, 0xd7, 0x70, 0x45, 0xdf,
	0x85, 0xc2, 0x32, 0xc5, 0xd0, 0x32, 0x94, 0x20, 0x66, 0x72, 0x1f, 0x2d, 0x7f, 0x11, 0x6f, 0x3c,
	0x79, 0x5f, 0x6f, 0x83, 0x9e, 0x7f, 0x03, 0x00, 0x00, 0xff, 0xff, 0xd9, 0xc9, 0x7a, 0x18, 0xde,
	0x00, 0x00, 0x00,
}
