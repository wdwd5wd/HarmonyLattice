package node

import (
	"github.com/ethereum/go-ethereum/rlp"
	proto_node "github.com/harmony-one/harmony/api/proto/node"
	"github.com/harmony-one/harmony/core/types"
	nodeconfig "github.com/harmony-one/harmony/internal/configs/node"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/p2p"
)

// BroadcastCXContractDIYPyramid 用于Pyramid，发送跨分片交易处理请求给对应分片，这里的size是总的size
func (node *Node) BroadcastCXContractDIYPyramid(blocknum uint64, shard0 uint32, shard1 uint32, step uint32, size int, totalnum uint64) {

	body := make([]byte, size)

	cxContract := &types.CXContract{
		BlockNum:    blocknum,
		Step:        step,
		Source:      shard0,
		Destination: shard1,
		Shard0:      shard0,
		Shard1:      shard1,
		Body:        body,
		TotalNum:    totalnum,
	}

	// groupID := nodeconfig.NewGroupIDByHorizontalShardID(nodeconfig.ShardID(shard1))
	groupID := nodeconfig.NewGroupIDByShardID(nodeconfig.ShardID(shard1))
	utils.Logger().Info().Uint32("ToHorizontalShardID", shard1).
		Str("GroupID", string(groupID)).
		// Interface("cxSC", cxContract).
		Msg("[BroadcastCXContractDIYPyramid] Sending CX smart contracts...")
	// TODO ek – limit concurrency
	go node.host.SendMessageToGroups([]nodeconfig.GroupID{groupID},
		p2p.ConstructMessage(proto_node.ConstructCXContractPyramid(cxContract)),
	)
}

func (node *Node) ProcessCXContractDIYPyramid(msgPayload []byte) {

	cxp := types.CXContract{}
	if err := rlp.DecodeBytes(msgPayload, &cxp); err != nil {
		utils.Logger().Error().Err(err).
			Msg("[ProcessCXContractDIYPyramid] Unable to Decode message Payload")
		return
	}

	// 不管收到几个触发执行智能合约的请求，每个node只执行一次
	if BlockNumProcessContract != cxp.BlockNum {
		BlockNumProcessContract = cxp.BlockNum
		utils.Logger().Debug().
			// Interface("cxp", cxp).
			Msg("[ProcessCXContractDIYPyramid] Processing cross-shard contract")

		// 先全部发送给subgroup
		// 舍弃了subgroup的design，改为在全组内达成共识
		for i := uint64(0); i < cxp.TotalNum; i++ {
			// 调整，Pyramid
			node.BroadcastCXContractDIYY(cxp.BlockNum, cxp.Shard0, cxp.Shard1, 2, 512*4096, cxp.TotalNum, i)
		}

	}

}
