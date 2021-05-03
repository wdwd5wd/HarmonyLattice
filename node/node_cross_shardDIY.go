// 各种测试算法在这个包

package node

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	proto_node "github.com/harmony-one/harmony/api/proto/node"
	"github.com/harmony-one/harmony/core/types"
	nodeconfig "github.com/harmony-one/harmony/internal/configs/node"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/p2p"
	"github.com/harmony-one/harmony/shard"
)

const TotalNumLimit = 10

var BlockNumProcessContract = uint64(0)

var CountCallContract = uint64(0)

var BlockNumCallContract = make([]uint64, TotalNumLimit)
var BlockNumProcessResult = make([]uint64, TotalNumLimit)
var BlockNumCallContractAgg = make([]uint64, TotalNumLimit)

var QuorumProcessContract = int64(0)
var QuorumCallContract = make([]int64, TotalNumLimit)
var QuorumProcessResult = make([]int64, TotalNumLimit)

var StepCallContract = make([]uint32, TotalNumLimit)
var StepCallContractAgg = make([]uint32, TotalNumLimit)

var BNumProConMap = make(map[common.Hash]uint64, TotalNumLimit)

// var BNumCallConMap = make(map[common.Hash]uint64, TotalNumLimit)
var BNumCallConAggMap = make(map[common.Hash]uint64, TotalNumLimit)
var BNumProResultMap = make(map[common.Hash]uint64, TotalNumLimit)

var QuoCallConMap = make(map[common.Hash]int64, TotalNumLimit)

// var StepCallConMap = make(map[common.Hash]uint32, TotalNumLimit)
// var StepCallConAggMap = make(map[common.Hash]uint32, TotalNumLimit)

var CountCallConMap = make(map[common.Hash]uint64, TotalNumLimit)

// BroadcastCXContractOnlyDIY broadcasts cross shard contract to correspoding
// destination shards
func (node *Node) BroadcastCXContractOnlyDIY(newBlock *types.Block) {

	epoch := newBlock.Header().Epoch()
	shardingConfig := shard.Schedule.InstanceForEpoch(epoch)
	shardNum := int(shardingConfig.NumShards())
	myShardID := node.Consensus.ShardID
	utils.Logger().Info().Int("shardNum", shardNum).Uint32("myShardID", myShardID).Uint64("blockNum", newBlock.NumberU64()).Msg("[BroadcastCXOnlyDIY]")

	for i := 0; i < shardNum; i++ {
		if i == int(myShardID) {
			continue
		}
		node.BroadcastCXContractWithShardIDDIY(newBlock, uint32(i))
	}
}

// BroadcastCXContractWithShardIDDIY broadcasts cross shard contracts to given ToShardID
func (node *Node) BroadcastCXContractWithShardIDDIY(block *types.Block, toShardID uint32) {
	myShardID := node.Consensus.ShardID
	utils.Logger().Debug().
		Uint32("toShardID", toShardID).
		Uint32("myShardID", myShardID).
		Uint64("blockNum", block.NumberU64()).
		Msg("[BroadcastCXWithShardIDDIY]")

	cxReceipts, err := node.Blockchain().ReadCXReceipts(toShardID, block.NumberU64(), block.Hash())
	if err != nil || len(cxReceipts) == 0 {
		utils.Logger().Debug().Uint32("ToShardID", toShardID).
			Int("numCXReceipts", len(cxReceipts)).
			Msg("[CXMerkleProof] No receipts found for the destination shard")
		return
	}

	// 我改了，记录广播时间
	if node.Consensus.IsLeader() {

		fmt.Println("CXBroadcastTime,", time.Now().UnixNano())
	}

	// 我改了，增加函数发送跨链智能合约
	// 调整，Baseline
	node.BroadcastCXContractDIY(block.NumberU64(), myShardID, toShardID, 3, 100*4096, 3)
}

// BroadcastCXReceiptsDIY broadcasts cross shard receipts to correspoding
// destination shards
func (node *Node) BroadcastCXReceiptsDIY(newBlock *types.Block) {
	commitSigAndBitmap := newBlock.GetCurrentCommitSig()
	//#### Read payload data from committed msg
	if len(commitSigAndBitmap) <= 96 {
		utils.Logger().Debug().Int("commitSigAndBitmapLen", len(commitSigAndBitmap)).Msg("[BroadcastCXReceiptsDIY] commitSigAndBitmap Not Enough Length")
		return
	}
	commitSig := make([]byte, 96)
	commitBitmap := make([]byte, len(commitSigAndBitmap)-96)
	offset := 0
	copy(commitSig[:], commitSigAndBitmap[offset:offset+96])
	offset += 96
	copy(commitBitmap[:], commitSigAndBitmap[offset:])
	//#### END Read payload data from committed msg

	epoch := newBlock.Header().Epoch()
	shardingConfig := shard.Schedule.InstanceForEpoch(epoch)
	shardNum := int(shardingConfig.NumShards())
	myShardID := node.Consensus.ShardID
	utils.Logger().Info().Int("shardNum", shardNum).Uint32("myShardID", myShardID).Uint64("blockNum", newBlock.NumberU64()).Msg("[BroadcastCXReceiptsDIY]")

	for i := 0; i < shardNum; i++ {
		if i == int(myShardID) {
			continue
		}
		node.BroadcastCXReceiptsWithShardIDDIY(newBlock, commitSig, commitBitmap, uint32(i))
	}
}

// BroadcastCXReceiptsWithShardIDDIY broadcasts cross shard receipts to given ToShardID
func (node *Node) BroadcastCXReceiptsWithShardIDDIY(block *types.Block, commitSig []byte, commitBitmap []byte, toShardID uint32) {
	myShardID := node.Consensus.ShardID
	utils.Logger().Debug().
		Uint32("ToHorizontalShardID", toShardID).
		Uint32("myShardID", myShardID).
		Uint64("blockNum", block.NumberU64()).
		Msg("[BroadcastCXReceiptsWithShardIDDIY]")

	cxReceipts, err := node.Blockchain().ReadCXReceipts(toShardID, block.NumberU64(), block.Hash())
	if err != nil || len(cxReceipts) == 0 {
		utils.Logger().Debug().Uint32("ToShardID", toShardID).
			Int("numCXReceipts", len(cxReceipts)).
			Msg("[CXMerkleProof] No receipts found for the destination shard")
		return
	}

	merkleProof, err := node.Blockchain().CXMerkleProof(toShardID, block)
	if err != nil {
		utils.Logger().Warn().
			Uint32("ToShardID", toShardID).
			Msg("[BroadcastCXReceiptsWithShardIDDIY] Unable to get merkleProof")
		return
	}

	cxReceiptsProof := &types.CXReceiptsProof{
		Receipts:     cxReceipts,
		MerkleProof:  merkleProof,
		Header:       block.Header(),
		CommitSig:    commitSig,
		CommitBitmap: commitBitmap,
	}

	groupID := nodeconfig.NewGroupIDByShardID(nodeconfig.ShardID(toShardID))
	utils.Logger().Info().Uint32("ToShardID", toShardID).
		Str("GroupID", string(groupID)).
		Interface("cxp", cxReceiptsProof).
		Msg("[BroadcastCXReceiptsWithShardIDDIY] ReadCXReceipts and MerkleProof ready. Sending CX receipts...")
	// TODO ek – limit concurrency
	go node.host.SendMessageToGroups([]nodeconfig.GroupID{groupID},
		p2p.ConstructMessage(proto_node.ConstructCXReceiptsProof(cxReceiptsProof)),
	)

	if node.Consensus.IsLeader() {

		fmt.Println("CXBroadcastTime,", time.Now().UnixNano())
	}

	// 增加函数发送跨链智能合约
	// // 调整，Pyramid
	// node.BroadcastCXContractDIYPyramid(block.NumberU64(), myShardID, toShardID, 2, 1000*4096, 3)

	// 调整，Lattice, final version?, 包括我们的（step=2），CxFunc(step增多)，SingleShard（跟我们差不多），yanking（跟CxFunc差不多，size变大）
	totalNum := 3
	for i := 0; i < totalNum; i++ {
		node.BroadcastCXContractDIYY(block.NumberU64(), myShardID, toShardID, 2, 1000*4096/totalNum, uint64(totalNum), uint64(i))
	}
}

// BroadcastCXContractOnlyDIYLattice broadcasts cross shard contract to correspoding
// destination shards
func (node *Node) BroadcastCXContractOnlyDIYLattice(newBlock *types.Block) {

	epoch := newBlock.Header().Epoch()
	shardingConfig := shard.Schedule.InstanceForEpoch(epoch)
	shardNum := int(shardingConfig.NumShards())
	myShardID := node.Consensus.ShardID
	utils.Logger().Info().Int("shardNum", shardNum).Uint32("myShardID", myShardID).Uint64("blockNum", newBlock.NumberU64()).Msg("[BroadcastCXOnlyDIYLattice]")

	for i := 0; i < shardNum; i++ {
		if i == int(myShardID) {
			continue
		}
		// 只让位于交叉位置subgroup的node发送信息
		if node.NodeConfig.GetHorizontalShardID() == uint32(i) {
			node.BroadcastCXContractWithShardIDDIYLattice(newBlock, uint32(i))

		}
	}
}

// BroadcastCXContractWithShardIDDIYLattice broadcasts cross shard contracts to given ToShardID
func (node *Node) BroadcastCXContractWithShardIDDIYLattice(block *types.Block, toShardID uint32) {
	myShardID := node.Consensus.ShardID
	utils.Logger().Debug().
		Uint32("ToHorizontalShardID", toShardID).
		Uint32("myShardID", myShardID).
		Uint64("blockNum", block.NumberU64()).
		Msg("[BroadcastCXWithShardIDDIYLattice]")

	cxReceipts, err := node.Blockchain().ReadCXReceipts(toShardID, block.NumberU64(), block.Hash())
	if err != nil || len(cxReceipts) == 0 {
		utils.Logger().Debug().Uint32("ToShardID", toShardID).
			Int("numCXReceipts", len(cxReceipts)).
			Msg("[CXMerkleProof] No receipts found for the destination shard")
		return
	}

	// 我改了，记录广播时间
	if node.Consensus.IsLeader() {

		fmt.Println("CXBroadcastTime,", time.Now().UnixNano())
	}

	// 我改了，增加函数发送跨链智能合约
	// 调整，Lattice，both basic and agg
	node.BroadcastCXContractDIYLattice(block.NumberU64(), myShardID, toShardID, 5, 512*4096, 5)

}

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
		// Interface("cxSC", cxContract).
		Msg("[BroadcastCXContractDIY] Sending CX smart contracts...")
	// TODO ek – limit concurrency
	go node.host.SendMessageToGroups([]nodeconfig.GroupID{groupID},
		p2p.ConstructMessage(proto_node.ConstructCXContractDIY(cxContract)),
	)
}

// BroadcastCXContractDIYLattice 用于Lattice，发送跨分片交易处理请求给对应分片，这里的size是总的size
func (node *Node) BroadcastCXContractDIYLattice(blocknum uint64, shard0 uint32, shard1 uint32, step uint32, size int, totalnum uint64) {

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
		Msg("[BroadcastCXContractDIYLattice] Sending CX smart contracts...")
	// TODO ek – limit concurrency
	go node.host.SendMessageToGroups([]nodeconfig.GroupID{groupID},
		p2p.ConstructMessage(proto_node.ConstructCXContractDIY(cxContract)),
	)
}

func (node *Node) ProcessCXContractMessageDIY(msgPayload []byte) {

	utils.Logger().Debug().
		Msg("[ProcessCXContractMessageDIY] Received CX Contract")

	cxp := types.CXContract{}
	if err := rlp.DecodeBytes(msgPayload, &cxp); err != nil {
		utils.Logger().Error().Err(err).
			Msg("[ProcessCXContractMessageDIY] Unable to Decode message Payload")
		return
	}

	// // 对于每一个智能合约，只有当收到足够数量的智能合约运行结果，才触发调用或返回结果，且每个node只执行一次
	// if BlockNumProcessContract != cxp.BlockNum {
	// 	QuorumProcessContract = int64(0)
	// 	BlockNumProcessContract = cxp.BlockNum
	// }

	// if QuorumProcessContract > int64(2*node.Consensus.Decider.ParticipantsCount()/3) {
	// 	// utils.Logger().Debug().Int64("ParticipantsCount", node.Consensus.Decider.ParticipantsCount()).
	// 	// 	Int64("Quorum", QuorumCallContract[selfnumInt]).
	// 	// 	Interface("cxp", cxp).
	// 	// 	Msg("[OnCalledCXContractDIY] Enough Quorum")
	// 	return
	// }

	// QuorumProcessContract = QuorumProcessContract + int64(1)

	// if QuorumProcessContract > int64(2*node.Consensus.Decider.ParticipantsCount()/3) {
	// 	utils.Logger().Debug().
	// 		// Interface("cxp", cxp).
	// 		Msg("[ProcessCXContractMessageDIY] Processing cross-shard contract")

	// 	if cxp.Step > uint32(1) {
	// 		for i := uint64(0); i < cxp.TotalNum; i++ {
	// 			// 调整，Baseline
	// 			node.CallAnotherCXContractDIY(cxp.BlockNum, cxp.Shard1, cxp.Shard0, cxp.Step-1, 100*4096, cxp.TotalNum, i)
	// 		}
	// 	}

	// 	if cxp.Step == uint32(1) {
	// 		for i := uint64(0); i < cxp.TotalNum; i++ {
	// 			// 调整，Baseline
	// 			node.ReturnCXContractResultDIY(cxp.BlockNum, cxp.Shard1, cxp.Shard0, cxp.Step, 100*4096, cxp.TotalNum, i)
	// 		}
	// 	}

	// }

	// 不管收到几个触发执行智能合约的请求，每个node只执行一次
	if BlockNumProcessContract != cxp.BlockNum {
		BlockNumProcessContract = cxp.BlockNum
		utils.Logger().Debug().
			// Interface("cxp", cxp).
			Msg("[ProcessCXContractMessageDIY] Processing cross-shard contract")

		if cxp.Step > uint32(1) {
			for i := uint64(0); i < cxp.TotalNum; i++ {
				// 调整，Baseline
				node.CallAnotherCXContractDIY(cxp.BlockNum, cxp.Shard1, cxp.Shard0, cxp.Step-1, 4096, cxp.TotalNum, i)
			}
		}

		if cxp.Step == uint32(1) {
			for i := uint64(0); i < cxp.TotalNum; i++ {
				// 调整，Baseline
				node.ReturnCXContractResultDIY(cxp.BlockNum, cxp.Shard1, cxp.Shard0, cxp.Step, 4096, cxp.TotalNum, i)
			}
		}

	}

}

func (node *Node) ProcessCXContractMessageDIYLattice(msgPayload []byte) {

	cxp := types.CXContract{}
	if err := rlp.DecodeBytes(msgPayload, &cxp); err != nil {
		utils.Logger().Error().Err(err).
			Msg("[ProcessCXContractMessageDIYLattice] Unable to Decode message Payload")
		return
	}

	// 不管收到几个触发执行智能合约的请求，每个node只执行一次

	if BlockNumProcessContract != cxp.BlockNum {
		BlockNumProcessContract = cxp.BlockNum

		utils.Logger().Debug().
			// Interface("cxp", cxp).
			Msg("[ProcessCXContractMessageDIYLattice] Processing cross-shard contract")

		if cxp.Step > uint32(1) {
			for i := uint64(0); i < cxp.TotalNum; i++ {
				// 调整，Lattice, basic
				node.CallAnotherCXContractDIYLattice(cxp.BlockNum, cxp.Source, cxp.Destination, cxp.Shard1, cxp.Shard0, cxp.Step-1, 4096, cxp.TotalNum, i)
			}
		}

		if cxp.Step == uint32(1) {
			for i := uint64(0); i < cxp.TotalNum; i++ {
				// 调整，Lattice, basic
				node.ReturnCXContractResultDIYLattice(cxp.BlockNum, cxp.Source, cxp.Destination, cxp.Shard1, cxp.Shard0, cxp.Step, 4096, cxp.TotalNum, i)
			}
		}

		// // 实现多个smart contract都在一个分片内执行
		// // 舍弃了
		// for {
		// 	utils.Logger().Debug().
		// 		// Interface("cxp", cxp).
		// 		Msg("[ProcessCXContractMessageDIYLattice] Processing cross-shard contract")

		// 	if cxp.Step > uint32(1) {
		// 		cxp.Step = cxp.Step - uint32(1)
		// 	}

		// 	if cxp.Step == uint32(1) {
		// 		for i := uint64(0); i < cxp.TotalNum; i++ {
		// 			// 调整，Lattice, basic
		// 			node.ReturnCXContractResultDIYLattice(cxp.BlockNum, cxp.Shard1, cxp.Shard0, cxp.Step, 100*4096, cxp.TotalNum, i)
		// 		}

		// 		break
		// 	}
		// }

	}

}

func (node *Node) ProcessCXContractDIYLatticeAgg(msgPayload []byte) {

	cxp := types.CXContract{}
	if err := rlp.DecodeBytes(msgPayload, &cxp); err != nil {
		utils.Logger().Error().Err(err).
			Msg("[ProcessCXContractDIYLatticeAgg] Unable to Decode message Payload")
		return
	}

	// 不管收到几个触发执行智能合约的请求，每个node只执行一次
	if BlockNumProcessContract != cxp.BlockNum {
		BlockNumProcessContract = cxp.BlockNum
		utils.Logger().Debug().
			// Interface("cxp", cxp).
			Msg("[ProcessCXContractDIYLatticeAgg] Processing cross-shard contract")

		// 先全部发送给subgroup
		// 舍弃了subgroup的design，改为在全组内达成共识
		for i := uint64(0); i < cxp.TotalNum; i++ {
			// 调整，Lattice, agg
			node.SendtoSubGroupDIYLatticeAgg(cxp.BlockNum, cxp.Source, cxp.Destination, cxp.Shard0, cxp.Shard1, cxp.Step, 300, cxp.TotalNum, i)
		}

	}

	// // 不管收到几个触发执行智能合约的请求，每个node只执行一次
	// z := new(big.Int)
	// z.SetBytes(msgPayload)
	// cxpHash := common.BigToHash(z)
	// // cxpHash := common.BytesToHash(msgPayload)

	// if _, ok := BNumProConMap[cxpHash]; !ok {
	// 	BNumProConMap[cxpHash] = cxp.BlockNum
	// 	utils.Logger().Debug().
	// 		Interface("cxp", cxpHash).
	// 		Msg("[ProcessCXContractDIYLatticeAgg] Processing cross-shard contract")

	// 	// 先全部发送给subgroup
	// 	// 舍弃了subgroup的design，改为在全组内达成共识
	// 	for i := uint64(0); i < cxp.TotalNum; i++ {
	// 		// 调整，Lattice, agg
	// 		node.SendtoSubGroupDIYLatticeAgg(cxp.BlockNum, cxp.Source, cxp.Destination, cxp.Shard0, cxp.Shard1, cxp.Step, 300, cxp.TotalNum, i)
	// 	}

	// 	// // 等待一段时间，将map删除以释放内存
	// 	// go func() {
	// 	// 	time.Sleep(60 * time.Second)
	// 	// 	delete(BNumProConMap, cxpHash)
	// 	// }()

	// }

}

// 舍弃了subgroup的design，改为在全组内达成共识
func (node *Node) SendtoSubGroupDIYLatticeAgg(blocknum uint64, source uint32, destination uint32, shard0 uint32, shard1 uint32, step uint32, size int, totalnum uint64, selfnum uint64) {

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

func (node *Node) OnSubGroupContractDIYLatticeAgg(msgPayload []byte) {

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

	// // 对于每一个智能合约，只有当收到足够数量的智能合约运行结果，才触发调用或返回结果，且每个node只执行一次
	// z := new(big.Int)
	// z.SetBytes(msgPayload)
	// cxpHash := common.BigToHash(z)
	// // cxpHash := common.BytesToHash(msgPayload)

	// if _, ok := QuoCallConMap[cxpHash]; !ok {
	// 	// BNumCallConMap[cxpHash] = cxp.BlockNum
	// 	// StepCallConMap[cxpHash] = cxp.Step
	// 	QuoCallConMap[cxpHash] = int64(0)

	// 	utils.Logger().Debug().
	// 		Interface("cxp", cxpHash).
	// 		Msg("[OnSubGroupContractDIYLatticeAgg] Here 1 ")
	// }

	// // if cxp.Step < StepCallConMap[cxpHash] {
	// // 	StepCallConMap[cxpHash] = cxp.Step
	// // 	QuoCallConMap[cxpHash] = int64(0)

	// // 	utils.Logger().Debug().
	// // 		// Interface("cxp", cxp).
	// // 		Msg("[OnSubGroupContractDIYLatticeAgg] Here 2 ")
	// // }

	// if QuoCallConMap[cxpHash] > int64(2*node.Consensus.Decider.ParticipantsCount()/3) {
	// 	// utils.Logger().Debug().Int64("ParticipantsCount", node.Consensus.Decider.ParticipantsCount()).
	// 	// 	Int64("Quorum", QuorumCallContract[selfnumInt]).
	// 	// 	Interface("cxp", cxp).
	// 	// 	Msg("[OnCalledCXContractDIYLattice] Enough Quorum")
	// 	return
	// }

	// QuoCallConMap[cxpHash] = QuoCallConMap[cxpHash] + int64(1)

	// utils.Logger().Debug().
	// 	// Interface("cxp", cxpHash).
	// 	Msg("[OnSubGroupContractDIYLatticeAgg] Here 3 ")

	// if QuoCallConMap[cxpHash] > int64(2*node.Consensus.Decider.ParticipantsCount()/3) {
	// 	// StepCallConMap[cxpHash] = cxp.Step - 1

	// 	cxContract := &types.CXContract{
	// 		BlockNum:    cxp.BlockNum,
	// 		Step:        cxp.Step,
	// 		Source:      cxp.Source,
	// 		Destination: cxp.Destination,
	// 		Shard0:      cxp.Shard0,
	// 		Shard1:      cxp.Shard1,
	// 		Body:        cxp.Body,
	// 		TotalNum:    cxp.TotalNum,
	// 	}

	// 	cxpByte, _ := rlp.EncodeToBytes(cxContract)
	// 	// cxpByteHash := common.BytesToHash(cxpByte)
	// 	ztemp := new(big.Int)
	// 	ztemp.SetBytes(cxpByte)
	// 	cxpByteHash := common.BigToHash(ztemp)

	// 	if _, ok := CountCallConMap[cxpByteHash]; !ok {
	// 		CountCallConMap[cxpByteHash] = 0
	// 	}

	// 	utils.Logger().Debug().
	// 		Interface("cxp", cxpByteHash).
	// 		Msg("[OnSubGroupContractDIYLatticeAgg] Here 4 ")

	// 	CountCallConMap[cxpByteHash] = CountCallConMap[cxpByteHash] + uint64(1)
	// 	if CountCallConMap[cxpByteHash] == cxp.TotalNum {

	// 		utils.Logger().Debug().
	// 			// Interface("cxp", cxp).
	// 			Msg("[OnSubGroupContractDIYLatticeAgg] Processing cross-shard contract")

	// 		// 只让leader发
	// 		if node.Consensus.IsLeader() {
	// 			if cxp.Step > uint32(1) {
	// 				// 调整，Lattice, agg
	// 				node.CallAnotherCXContractDIYLattice(cxp.BlockNum, cxp.Source, cxp.Destination, cxp.Shard1, cxp.Shard0, cxp.Step-1, 512*4096, 1, 0)
	// 			}
	// 			if cxp.Step == uint32(1) {
	// 				// 调整，Lattice, agg
	// 				node.ReturnCXContractResultDIYLattice(cxp.BlockNum, cxp.Source, cxp.Destination, cxp.Shard1, cxp.Shard0, cxp.Step, 512*4096, 1, 0)
	// 			}
	// 		}
	// 		CountCallConMap[cxpByteHash] = 0

	// 		// // 等待一段时间，将map删除以释放内存
	// 		// go func() {
	// 		// 	time.Sleep(60 * time.Second)
	// 		// 	// delete(BNumCallConMap, cxpHash)
	// 		// 	// delete(StepCallConMap, cxpHash)
	// 		// 	delete(QuoCallConMap, cxpHash)
	// 		// 	delete(CountCallConMap, cxpByteHash)
	// 		// }()

	// 	}
	// }

}

func (node *Node) OnCalledCXContractDIY(msgPayload []byte) {

	utils.Logger().Debug().
		Msg("[OnCalledCXContractDIY] Received On Called CX Contract")

	cxp := types.CXContract{}
	if err := rlp.DecodeBytes(msgPayload, &cxp); err != nil {
		utils.Logger().Error().Err(err).
			Msg("[OnCalledCXContractDIY] Unable to Decode message Payload")
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
		// 	Msg("[OnCalledCXContractDIY] Enough Quorum")
		return
	}

	QuorumCallContract[selfnumInt] = QuorumCallContract[selfnumInt] + int64(1)

	if QuorumCallContract[selfnumInt] > int64(2*node.Consensus.Decider.ParticipantsCount()/3) {
		StepCallContract[selfnumInt] = cxp.Step - 1

		CountCallContract = CountCallContract + uint64(1)
		if CountCallContract == cxp.TotalNum {
			utils.Logger().Debug().
				// Interface("cxp", cxp).
				Msg("[OnCalledCXContractDIY] Processing cross-shard contract")

			if cxp.Step > uint32(1) {
				// 调整，Baseline
				node.CallAnotherCXContractDIY(cxp.BlockNum, cxp.Shard1, cxp.Shard0, cxp.Step-1, 4096, 1, 0)
			}
			if cxp.Step == uint32(1) {
				// 调整，Baseline
				node.ReturnCXContractResultDIY(cxp.BlockNum, cxp.Shard1, cxp.Shard0, cxp.Step, 4096, 1, 0)
			}
			CountCallContract = 0

		}

	}

}

func (node *Node) OnCalledCXContractDIYLattice(msgPayload []byte) {

	cxp := types.CXContract{}
	if err := rlp.DecodeBytes(msgPayload, &cxp); err != nil {
		utils.Logger().Error().Err(err).
			Msg("[OnCalledCXContractDIYLattice] Unable to Decode message Payload")
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

			// // 原始的设计，需要smart contract跨分片调用
			// utils.Logger().Debug().
			// 	// Interface("cxp", cxp).
			// 	Msg("[OnCalledCXContractDIYLattice] Processing cross-shard contract")

			// if cxp.Step > uint32(1) {
			// 	// 调整，Lattice, basic
			// 	node.CallAnotherCXContractDIYLattice(cxp.BlockNum, cxp.Shard1, cxp.Shard0, cxp.Step-1, 4096, 1, 0)
			// }
			// if cxp.Step == uint32(1) {
			// 	// 调整，Lattice, basic
			// 	node.ReturnCXContractResultDIYLattice(cxp.BlockNum, cxp.Shard1, cxp.Shard0, cxp.Step, 4096, 1, 0)
			// }

			// 实现多个smart contract都在一个分片内执行
			for {
				utils.Logger().Debug().
					// Interface("cxp", cxp).
					Msg("[OnCalledCXContractDIYLattice] Processing cross-shard contract")

				if cxp.Step > uint32(1) {
					cxp.Step = cxp.Step - uint32(1)
				}

				if cxp.Step == uint32(1) {
					// 调整，Lattice, basic
					node.ReturnCXContractResultDIYLattice(cxp.BlockNum, cxp.Source, cxp.Destination, cxp.Shard1, cxp.Shard0, cxp.Step, 4096, 1, 0)
					break
				}
			}

			CountCallContract = 0
		}
	}

}

func (node *Node) OnCalledCXContractDIYLatticeAgg(msgPayload []byte) {

	cxp := types.CXContract{}
	if err := rlp.DecodeBytes(msgPayload, &cxp); err != nil {
		utils.Logger().Error().Err(err).
			Msg("[OnCalledCXContractDIYLatticeAgg] Unable to Decode message Payload")
		return
	}

	selfnumInt := int(cxp.SelfNum)
	// 对于每一个智能合约，每个node只执行一次
	if BlockNumCallContractAgg[selfnumInt] != cxp.BlockNum {
		BlockNumCallContractAgg[selfnumInt] = cxp.BlockNum
		StepCallContractAgg[selfnumInt] = cxp.Step

		utils.Logger().Debug().
			// Interface("cxp", cxp).
			Msg("[OnCalledCXContractDIYLatticeAgg] Processing cross-shard contract")

		// // 调整，Lattice, agg
		// // StepCallContractAgg[selfnumInt] = cxp.Step - 1
		// node.SendtoSubGroupDIYLatticeAgg(cxp.BlockNum, cxp.Source, cxp.Destination, cxp.Shard0, cxp.Shard1, cxp.Step, 300, 1, 0)

		// 调整，Lattice, final version?
		node.SendtoConsensusDIYY(cxp.BlockNum, cxp.Source, cxp.Destination, cxp.Shard0, cxp.Shard1, cxp.Step, 300, 1)

		return
	}

	if cxp.Step < StepCallContractAgg[selfnumInt] {
		StepCallContractAgg[selfnumInt] = cxp.Step

		utils.Logger().Debug().
			// Interface("cxp", cxp).
			Msg("[OnCalledCXContractDIYLatticeAgg] Processing cross-shard contract")

		// 调整，Lattice, agg
		// StepCallContractAgg[selfnumInt] = cxp.Step - 1
		node.SendtoSubGroupDIYLatticeAgg(cxp.BlockNum, cxp.Source, cxp.Destination, cxp.Shard0, cxp.Shard1, cxp.Step, 300, 1, 0)

		return
	}

	// // 对于每一个智能合约，每个node只执行一次
	// z := new(big.Int)
	// z.SetBytes(msgPayload)
	// cxpHash := common.BigToHash(z)
	// // cxpHash := common.BytesToHash(msgPayload)

	// if _, ok := BNumCallConAggMap[cxpHash]; !ok {
	// 	BNumCallConAggMap[cxpHash] = cxp.BlockNum
	// 	// StepCallConAggMap[cxpHash] = cxp.Step

	// 	utils.Logger().Debug().
	// 		// Interface("cxp", cxp).
	// 		Msg("[OnCalledCXContractDIYLatticeAgg] Processing cross-shard contract")

	// 	// 调整，Lattice, agg
	// 	node.SendtoSubGroupDIYLatticeAgg(cxp.BlockNum, cxp.Source, cxp.Destination, cxp.Shard0, cxp.Shard1, cxp.Step, 300, 1, 0)

	// 	// // 等待一段时间，将map删除以释放内存
	// 	// go func() {
	// 	// 	time.Sleep(60 * time.Second)
	// 	// 	delete(BNumCallConAggMap, cxpHash)
	// 	// 	// delete(StepCallConAggMap, cxpHash)
	// 	// }()

	// 	return
	// }

	// if cxp.Step < StepCallConAggMap[cxpHash] {
	// 	StepCallConAggMap[cxpHash] = cxp.Step

	// 	utils.Logger().Debug().
	// 		// Interface("cxp", cxp).
	// 		Msg("[OnCalledCXContractDIYLatticeAgg] Processing cross-shard contract")

	// 	// 调整，Lattice, agg
	// 	node.SendtoSubGroupDIYLatticeAgg(cxp.BlockNum, cxp.Source, cxp.Destination, cxp.Shard0, cxp.Shard1, cxp.Step, 300, 1, 0)

	// 	// 等待一段时间，将map删除以释放内存
	// 	go func() {
	// 		time.Sleep(60 * time.Second)
	// 		delete(BNumCallConAggMap, cxpHash)
	// 		delete(StepCallConAggMap, cxpHash)
	// 	}()

	// 	return
	// }

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
		// Interface("cxSC", cxContract).
		Msg("[CallAnotherCXContractDIY] Calling another CX smart contracts...")
	// TODO ek – limit concurrency
	go node.host.SendMessageToGroups([]nodeconfig.GroupID{groupID},
		p2p.ConstructMessage(proto_node.CallCXContractDIY(cxContract)),
	)
}

func (node *Node) CallAnotherCXContractDIYLattice(blocknum uint64, source uint32, destination uint32, shard0 uint32, shard1 uint32, step uint32, size int, totalnum uint64, selfnum uint64) {

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

	// groupID := nodeconfig.NewGroupIDByHorizontalShardID(nodeconfig.ShardID(shard1))
	groupID := nodeconfig.NewGroupIDByShardID(nodeconfig.ShardID(shard1))
	utils.Logger().Info().Uint32("ToHorizontalShardID", shard1).
		Str("GroupID", string(groupID)).
		// Interface("cxSC", cxContract).
		Msg("[CallAnotherCXContractDIYLattice] Calling another CX smart contracts...")
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
		// Interface("cxSC", cxContract).
		Msg("[ReturnCXContractResultDIY] Return CX smart contract results...")
	// TODO ek – limit concurrency
	go node.host.SendMessageToGroups([]nodeconfig.GroupID{groupID},
		p2p.ConstructMessage(proto_node.ConstructCXResultDIY(cxContract)),
	)
}

func (node *Node) ReturnCXContractResultDIYLattice(blocknum uint64, source uint32, destination uint32, shard0 uint32, shard1 uint32, step uint32, size int, totalnum uint64, selfnum uint64) {

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

	groupID := nodeconfig.NewGroupIDByShardID(nodeconfig.ShardID(shard1))
	utils.Logger().Info().Uint32("ToShardID", shard1).
		Str("GroupID", string(groupID)).
		// Interface("cxSC", cxContract).
		Msg("[ReturnCXContractResultDIYLattice] Return CX smart contract results...")
	// TODO ek – limit concurrency
	go node.host.SendMessageToGroups([]nodeconfig.GroupID{groupID},
		p2p.ConstructMessage(proto_node.ConstructCXResultDIY(cxContract)),
	)
}

func (node *Node) ProcessCXResultMessageDIY(msgPayload []byte) {

	utils.Logger().Debug().
		Msg("[ProcessCXResultMessageDIY] Received CX Contract Results")

	cxp := types.CXContract{}
	if err := rlp.DecodeBytes(msgPayload, &cxp); err != nil {
		utils.Logger().Error().Err(err).
			Msg("[ProcessCXResultMessageDIY] Unable to Decode message Payload")
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
		utils.Logger().Debug().
			// Interface("cxp", cxp).
			Int64("ParticipantsCount", node.Consensus.Decider.ParticipantsCount()).
			Int64("Quorum", QuorumProcessResult[selfnumInt]).
			Msg("[ProcessCXResultMessageDIY] Process cross-shard contract DONE")

		if node.Consensus.IsLeader() {
			fmt.Println("CXFinishTime,", time.Now().UnixNano())
		}
	}

}

func (node *Node) ProcessCXResultMessageDIYLattice(msgPayload []byte) {

	cxp := types.CXContract{}
	if err := rlp.DecodeBytes(msgPayload, &cxp); err != nil {
		utils.Logger().Error().Err(err).
			Msg("[ProcessCXResultMessageDIYLattice] Unable to Decode message Payload")
		return
	}

	selfnumInt := int(cxp.SelfNum)
	// 不管收到几个触发执行智能合约的请求，每个node只执行一次
	if BlockNumProcessResult[selfnumInt] != cxp.BlockNum {
		BlockNumProcessResult[selfnumInt] = cxp.BlockNum

		utils.Logger().Debug().
			// Interface("cxp", cxp).
			Int64("ParticipantsCount", node.Consensus.Decider.ParticipantsCount()).
			Int64("Quorum", QuorumProcessResult[selfnumInt]).
			Msg("[ProcessCXResultMessageDIYLattice] Process cross-shard contract DONE")

		if node.Consensus.IsLeader() {
			fmt.Println("CXFinishTime,", time.Now().UnixNano())
		}

	}

	// // 不管收到几个触发执行智能合约的请求，每个node只执行一次
	// z := new(big.Int)
	// z.SetBytes(msgPayload)
	// cxpHash := common.BigToHash(z)
	// // cxpHash := common.BytesToHash(msgPayload)

	// if _, ok := BNumProResultMap[cxpHash]; !ok {
	// 	BNumProResultMap[cxpHash] = cxp.BlockNum

	// 	utils.Logger().Debug().
	// 		// Interface("cxp", cxp).
	// 		// Int64("ParticipantsCount", node.Consensus.Decider.ParticipantsCount()).
	// 		// Int64("Quorum", QuorumProcessResult[selfnumInt]).
	// 		Msg("[ProcessCXResultMessageDIYLattice] Process cross-shard contract DONE")

	// 	if node.Consensus.IsLeader() {
	// 		fmt.Println("CXFinishTime,", time.Now().UnixNano())
	// 	}

	// 	// // 等待一段时间，将map删除以释放内存
	// 	// go func() {
	// 	// 	time.Sleep(60 * time.Second)
	// 	// 	delete(BNumProResultMap, cxpHash)
	// 	// }()

	// }

	// // 对于每一个智能合约，只有当收到足够数量的智能合约运行结果，才执行，且每个node只执行一次
	// if BlockNumProcessResult[selfnumInt] != cxp.BlockNum {
	// 	QuorumProcessResult[selfnumInt] = int64(0)
	// 	BlockNumProcessResult[selfnumInt] = cxp.BlockNum
	// }

	// if QuorumProcessResult[selfnumInt] > 2*node.Consensus.Decider.ParticipantsCount()/3 {
	// 	return
	// }

	// QuorumProcessResult[selfnumInt] = QuorumProcessResult[selfnumInt] + int64(1)

	// if QuorumProcessResult[selfnumInt] > 2*node.Consensus.Decider.ParticipantsCount()/3 {
	// 	utils.Logger().Debug().
	// 		// Interface("cxp", cxp).
	// 		Int64("ParticipantsCount", node.Consensus.Decider.ParticipantsCount()).
	// 		Int64("Quorum", QuorumProcessResult[selfnumInt]).
	// 		Msg("[ProcessCXResultMessageDIYLattice] Process cross-shard contract DONE")

	// 	if node.Consensus.IsLeader() {
	// 		fmt.Println("CXFinishTime,", time.Now().UnixNano())
	// 	}
	// }

}
