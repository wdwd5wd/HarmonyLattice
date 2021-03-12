package node

import (
	"github.com/ethereum/go-ethereum/rlp"
	proto_node "github.com/harmony-one/harmony/api/proto/node"
	"github.com/harmony-one/harmony/core/types"
	nodeconfig "github.com/harmony-one/harmony/internal/configs/node"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/p2p"
)

const TotalNumLimit = 10000

var BlockNumProcessContract = uint64(0)

var BlockNumCallContract = make([]uint64, TotalNumLimit)
var BlockNumProcessResult = make([]uint64, TotalNumLimit)

var QuorumCallContract = make([]int64, TotalNumLimit)
var QuorumProcessResult = make([]int64, TotalNumLimit)

var StepCallContract = make([]uint32, TotalNumLimit)

// var BitmapCallContract = make([]int, TotalNumLimit)
// var BitmapProcessResult = make([]int, TotalNumLimit)

// BroadcastCXContractDIY 发送跨分片交易处理请求给对应分片，这里的size是总的size
func (node *Node) BroadcastCXContractDIY(blocknum uint64, shard0 uint32, shard1 uint32, step uint32, size int, totalnum uint64) {

	body := make([]byte, size)

	cxContract := &types.CXContract{
		BlockNum: blocknum,
		Step:     step,
		Shard0:   shard0,
		Shard1:   shard1,
		Body:     body,
		TotalNum: totalnum,
	}

	groupID := nodeconfig.NewGroupIDByShardID(nodeconfig.ShardID(shard1))
	utils.Logger().Info().Uint32("ToShardID", shard1).
		Str("GroupID", string(groupID)).
		Interface("cxSC", cxContract).
		Msg("[BroadcastCXContract] Sending CX smart contracts...")
	// TODO ek – limit concurrency
	go node.host.SendMessageToGroups([]nodeconfig.GroupID{groupID},
		p2p.ConstructMessage(proto_node.ConstructCXContractDIY(cxContract)),
	)
}

func (node *Node) ProcessCXContractMessageDIY(msgPayload []byte) {

	cxp := types.CXContract{}
	if err := rlp.DecodeBytes(msgPayload, &cxp); err != nil {
		utils.Logger().Error().Err(err).
			Msg("[ProcessCXContractMessage] Unable to Decode message Payload")
		return
	}

	// 不管收到几个触发执行智能合约的请求，每个node只执行一次

	if BlockNumProcessContract != cxp.BlockNum {
		utils.Logger().Debug().Interface("cxp", cxp).
			Msg("[ProcessCXContractMessage] Processing cross-shard contract")

		if cxp.Step > uint32(0) {
			for i := uint64(0); i < cxp.TotalNum; i++ {
				node.CallAnotherCXContractDIY(cxp.BlockNum, cxp.Shard1, cxp.Shard0, cxp.Step-1, 200, cxp.TotalNum, i)
			}
		}

		if cxp.Step == uint32(0) {
			for i := uint64(0); i < cxp.TotalNum; i++ {
				node.ReturnCXContractResultDIY(cxp.BlockNum, cxp.Shard1, cxp.Shard0, cxp.Step, 300, cxp.TotalNum, i)
			}
		}

		BlockNumProcessContract = cxp.BlockNum
	}

}

func (node *Node) OnCalledCXContractDIY(msgPayload []byte) {

	cxp := types.CXContract{}
	if err := rlp.DecodeBytes(msgPayload, &cxp); err != nil {
		utils.Logger().Error().Err(err).
			Msg("[OnCalledCXContract] Unable to Decode message Payload")
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
		utils.Logger().Debug().Int64("ParticipantsCount", node.Consensus.Decider.ParticipantsCount()).
			Int64("Quorum", QuorumCallContract[selfnumInt]).
			Interface("cxp", cxp).
			Msg("[OnCalledCXContract] Enough Quorum")
		return
	}

	QuorumCallContract[selfnumInt] = QuorumCallContract[selfnumInt] + int64(1)

	if QuorumCallContract[selfnumInt] > int64(2*node.Consensus.Decider.ParticipantsCount()/3) {
		utils.Logger().Debug().Interface("cxp", cxp).
			Msg("[OnCalledCXContract] Processing cross-shard contract")

		if cxp.Step > uint32(0) {
			node.CallAnotherCXContractDIY(cxp.BlockNum, cxp.Shard1, cxp.Shard0, cxp.Step-1, 200, cxp.TotalNum, cxp.SelfNum)
			StepCallContract[selfnumInt] = cxp.Step - 1
		}
		if cxp.Step == uint32(0) {
			node.ReturnCXContractResultDIY(cxp.BlockNum, cxp.Shard1, cxp.Shard0, cxp.Step, 300, cxp.TotalNum, cxp.SelfNum)
		}
	}

}

func (node *Node) CallAnotherCXContractDIY(blocknum uint64, shard0 uint32, shard1 uint32, step uint32, size int, totalnum uint64, selfnum uint64) {

	body := make([]byte, size)

	cxContract := &types.CXContract{
		BlockNum: blocknum,
		Step:     step,
		Shard0:   shard0,
		Shard1:   shard1,
		Body:     body,
		TotalNum: totalnum,
		SelfNum:  selfnum,
	}

	groupID := nodeconfig.NewGroupIDByShardID(nodeconfig.ShardID(shard1))
	utils.Logger().Info().Uint32("ToShardID", shard1).
		Str("GroupID", string(groupID)).
		Interface("cxSC", cxContract).
		Msg("[CallAnotherCXContract] Calling another CX smart contracts...")
	// TODO ek – limit concurrency
	go node.host.SendMessageToGroups([]nodeconfig.GroupID{groupID},
		p2p.ConstructMessage(proto_node.CallCXContractDIY(cxContract)),
	)
}

func (node *Node) ReturnCXContractResultDIY(blocknum uint64, shard0 uint32, shard1 uint32, step uint32, size int, totalnum uint64, selfnum uint64) {

	body := make([]byte, size)

	cxContract := &types.CXContract{
		BlockNum: blocknum,
		Step:     step,
		Shard0:   shard0,
		Shard1:   shard1,
		Body:     body,
		TotalNum: totalnum,
		SelfNum:  selfnum,
	}

	groupID := nodeconfig.NewGroupIDByShardID(nodeconfig.ShardID(shard1))
	utils.Logger().Info().Uint32("ToShardID", shard1).
		Str("GroupID", string(groupID)).
		Interface("cxSC", cxContract).
		Msg("[ReturnCXContractResult] Return CX smart contract results...")
	// TODO ek – limit concurrency
	go node.host.SendMessageToGroups([]nodeconfig.GroupID{groupID},
		p2p.ConstructMessage(proto_node.ConstructCXResultDIY(cxContract)),
	)
}

func (node *Node) ProcessCXResultMessageDIY(msgPayload []byte) {

	cxp := types.CXContract{}
	if err := rlp.DecodeBytes(msgPayload, &cxp); err != nil {
		utils.Logger().Error().Err(err).
			Msg("[ProcessCXResultMessage] Unable to Decode message Payload")
		return
	}

	selfnumInt := int(cxp.SelfNum)

	// 对于每一个智能合约，只有当收到足够数量的智能合约运行结果，才执行，且每个node只执行一次
	if BlockNumProcessResult[selfnumInt] != cxp.BlockNum {
		QuorumProcessResult[selfnumInt] = int64(0)
		BlockNumProcessResult[selfnumInt] = cxp.BlockNum
	}

	if QuorumProcessResult[selfnumInt] > 2*node.Consensus.Decider.ParticipantsCount()/3 {
		return
	}

	QuorumProcessResult[selfnumInt] = QuorumProcessResult[selfnumInt] + int64(1)

	if QuorumProcessResult[selfnumInt] > 2*node.Consensus.Decider.ParticipantsCount()/3 {
		utils.Logger().Debug().Interface("cxp", cxp).
			Int64("ParticipantsCount", node.Consensus.Decider.ParticipantsCount()).
			Int64("Quorum", QuorumProcessResult[selfnumInt]).
			Msg("[ProcessCXResultMessage] Process cross-shard contract DONE")
	}

}
