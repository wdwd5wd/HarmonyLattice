// 我们的协议在这个包

package node

import (
	"github.com/ethereum/go-ethereum/rlp"
	proto_node "github.com/harmony-one/harmony/api/proto/node"
	"github.com/harmony-one/harmony/core/types"
	nodeconfig "github.com/harmony-one/harmony/internal/configs/node"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/p2p"
)

var CountProConDIYY = uint64(0)
var BlockNumProConDIYY = make([]uint64, TotalNumLimit)

var BlockNumConsensus = uint64(0)
var QuorumConsensus = int64(0)
var StepConsensus = uint32(0)

// BroadcastCXContractDIYY 用于Lattice，逐步发送跨分片交易处理请求给对应分片，这里的size是单个size
func (node *Node) BroadcastCXContractDIYY(blocknum uint64, shard0 uint32, shard1 uint32, step uint32, size int, totalnum uint64, selfnum uint64) {

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
		SelfNum:     selfnum,
	}

	// groupID := nodeconfig.NewGroupIDByHorizontalShardID(nodeconfig.ShardID(shard1))
	groupID := nodeconfig.NewGroupIDByShardID(nodeconfig.ShardID(shard1))
	utils.Logger().Info().Uint32("ToHorizontalShardID", shard1).
		Str("GroupID", string(groupID)).
		// Interface("cxSC", cxContract).
		Msg("[BroadcastCXContractDIYY] Sending CX smart contracts...")
	// TODO ek – limit concurrency
	go node.host.SendMessageToGroups([]nodeconfig.GroupID{groupID},
		p2p.ConstructMessage(proto_node.ConstructCXContractDIY(cxContract)),
	)
}

func (node *Node) ProcessCXContractDIYY(msgPayload []byte) {

	cxp := types.CXContract{}
	if err := rlp.DecodeBytes(msgPayload, &cxp); err != nil {
		utils.Logger().Error().Err(err).
			Msg("[ProcessCXContractDIYY] Unable to Decode message Payload")
		return
	}

	selfnumInt := int(cxp.SelfNum)
	// 不管收到几个触发执行智能合约的请求，每个node只执行一次
	if BlockNumProConDIYY[selfnumInt] != cxp.BlockNum {
		BlockNumProConDIYY[selfnumInt] = cxp.BlockNum

		CountProConDIYY = CountProConDIYY + uint64(1)
		if CountProConDIYY == cxp.TotalNum {

			utils.Logger().Debug().
				// Interface("cxp", cxp).
				Msg("[ProcessCXContractDIYY] Processing cross-shard contract")

			// 在全组内达成共识
			// 调整，Lattice, final version?
			node.SendtoConsensusDIYY(cxp.BlockNum, cxp.Source, cxp.Destination, cxp.Shard0, cxp.Shard1, cxp.Step, 300, 1)

			CountProConDIYY = 0
		}

	}

}

func (node *Node) SendtoConsensusDIYY(blocknum uint64, source uint32, destination uint32, shard0 uint32, shard1 uint32, step uint32, size int, totalnum uint64) {

	body := make([]byte, size)

	cxContract := &types.CXContract{
		BlockNum:    blocknum,
		Step:        step,
		Source:      source,
		Destination: destination,
		Shard0:      shard0,
		Shard1:      shard1,
		Body:        body,
		TotalNum:    totalnum,
	}

	// groupID := nodeconfig.NewSubgroupIDByShardInfo(nodeconfig.ShardID(shard1), nodeconfig.ShardID(shard1))
	groupID := nodeconfig.NewGroupIDByShardID(nodeconfig.ShardID(shard1))
	utils.Logger().Info().Uint32("ToSubGroupShardID", shard1).
		Str("GroupID", string(groupID)).
		// Interface("cxSC", cxContract).
		Msg("[SendtoConsensusDIYY] Sending contract to Consensus...")
	// TODO ek – limit concurrency
	go node.host.SendMessageToGroups([]nodeconfig.GroupID{groupID},
		p2p.ConstructMessage(proto_node.CallConsensusDIYY(cxContract)),
	)
}

func (node *Node) OnConsensusDIYY(msgPayload []byte) {

	cxp := types.CXContract{}
	if err := rlp.DecodeBytes(msgPayload, &cxp); err != nil {
		utils.Logger().Error().Err(err).
			Msg("[OnConsensusDIYY] Unable to Decode message Payload")
		return
	}

	// selfnumInt := int(cxp.SelfNum)
	// 对于每一个智能合约，只有当收到足够数量的智能合约运行结果，才触发调用或返回结果，且每个node只执行一次
	if BlockNumConsensus != cxp.BlockNum {
		BlockNumConsensus = cxp.BlockNum
		QuorumConsensus = int64(0)
		StepConsensus = cxp.Step
	}

	if cxp.Step < StepConsensus {
		StepConsensus = cxp.Step
		QuorumConsensus = int64(0)
	}

	if QuorumConsensus > int64(2*node.Consensus.Decider.ParticipantsCount()/3) {
		// utils.Logger().Debug().Int64("ParticipantsCount", node.Consensus.Decider.ParticipantsCount()).
		// 	Int64("Quorum", QuorumCallContract[selfnumInt]).
		// 	Interface("cxp", cxp).
		// 	Msg("[OnCalledCXContractDIYLattice] Enough Quorum")
		return
	}

	QuorumConsensus = QuorumConsensus + int64(1)

	if QuorumConsensus > int64(2*node.Consensus.Decider.ParticipantsCount()/3) {

		utils.Logger().Debug().
			// Interface("cxp", cxp).
			Msg("[OnConsensusDIYY] Processing cross-shard contract")

		// 调整，Lattice, final version?
		node.SendtoSubGroupDIYLatticeAgg(cxp.BlockNum, cxp.Source, cxp.Destination, cxp.Shard0, cxp.Shard1, cxp.Step, 300, 1, 0)

	}

}

// 舍弃了subgroup的design，改为在全组内达成共识
func (node *Node) ConsensusRoundTwo(blocknum uint64, source uint32, destination uint32, shard0 uint32, shard1 uint32, step uint32, size int, totalnum uint64, selfnum uint64) {

	body := make([]byte, size)

	cxContract := &types.CXContract{
		BlockNum:    blocknum,
		Step:        step,
		Source:      source,
		Destination: destination,
		Shard0:      shard0,
		Shard1:      shard1,
		Body:        body,
		TotalNum:    totalnum,
		SelfNum:     selfnum,
	}

	// groupID := nodeconfig.NewSubgroupIDByShardInfo(nodeconfig.ShardID(shard1), nodeconfig.ShardID(shard1))
	groupID := nodeconfig.NewGroupIDByShardID(nodeconfig.ShardID(shard1))
	utils.Logger().Info().Uint32("ToSubGroupShardID", shard1).
		Str("GroupID", string(groupID)).
		// Interface("cxSC", cxContract).
		Msg("[SendtoSubGroupDIYLatticeAgg] Sending contract to SubGroup...")
	// TODO ek – limit concurrency
	go node.host.SendMessageToGroups([]nodeconfig.GroupID{groupID},
		p2p.ConstructMessage(proto_node.CallCXContractBySubGroupDIY(cxContract)),
	)

}

func (node *Node) OnConsensusRoundTwo(msgPayload []byte) {

	cxp := types.CXContract{}
	if err := rlp.DecodeBytes(msgPayload, &cxp); err != nil {
		utils.Logger().Error().Err(err).
			Msg("[OnSubGroupContractDIYLatticeAgg] Unable to Decode message Payload")
		return
	}

	selfnumInt := int(cxp.SelfNum)
	// 对于每一个智能合约，只有当收到足够数量的智能合约运行结果，才触发调用或返回结果，且每个node只执行一次
	if BlockNumCallContract[selfnumInt] != cxp.BlockNum {
		QuorumCallContract[selfnumInt] = int64(0)
		BlockNumCallContract[selfnumInt] = cxp.BlockNum
		StepCallContract[selfnumInt] = cxp.Step
	}

	if cxp.Step < StepCallContract[selfnumInt] {
		QuorumCallContract[selfnumInt] = int64(0)
		StepCallContract[selfnumInt] = cxp.Step
	}

	if QuorumCallContract[selfnumInt] > int64(2*node.Consensus.Decider.ParticipantsCount()/3) {
		// utils.Logger().Debug().Int64("ParticipantsCount", node.Consensus.Decider.ParticipantsCount()).
		// 	Int64("Quorum", QuorumCallContract[selfnumInt]).
		// 	Interface("cxp", cxp).
		// 	Msg("[OnCalledCXContractDIYLattice] Enough Quorum")
		return
	}

	QuorumCallContract[selfnumInt] = QuorumCallContract[selfnumInt] + int64(1)

	if QuorumCallContract[selfnumInt] > int64(2*node.Consensus.Decider.ParticipantsCount()/3) {
		StepCallContract[selfnumInt] = cxp.Step - 1

		CountCallContract = CountCallContract + uint64(1)
		if CountCallContract == cxp.TotalNum {

			utils.Logger().Debug().
				// Interface("cxp", cxp).
				Msg("[OnSubGroupContractDIYLatticeAgg] Processing cross-shard contract")

			// 只让leader发
			if node.Consensus.IsLeader() {
				if cxp.Step > uint32(1) {
					// 调整，Lattice, agg
					node.CallAnotherCXContractDIYLattice(cxp.BlockNum, cxp.Source, cxp.Destination, cxp.Shard1, cxp.Shard0, cxp.Step-1, 1000*4096, 1, 0)
				}
				if cxp.Step == uint32(1) {
					// 调整，Lattice, agg
					node.ReturnCXContractResultDIYLattice(cxp.BlockNum, cxp.Source, cxp.Destination, cxp.Shard1, cxp.Shard0, cxp.Step, 1000*4096, 1, 0)
				}
			}
			CountCallContract = 0

		}
	}

}
