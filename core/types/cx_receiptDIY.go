package types

// CXContract represents a cross-shard contract call
type CXContract struct {
	BlockNum    uint64
	Step        uint32
	Source      uint32
	Destination uint32
	Shard0      uint32
	Shard1      uint32
	Body        []byte
	TotalNum    uint64
	SelfNum     uint64
}
