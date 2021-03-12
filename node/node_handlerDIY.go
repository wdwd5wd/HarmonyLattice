package node

import (
	"github.com/harmony-one/harmony/consensus"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/shard"
	"github.com/harmony-one/harmony/staking/availability"
	"github.com/harmony-one/harmony/webhooks"
)

// PostConsensusProcessingDIY is called by consensus participants, after consensus is done, to:
// 1. add the new block to blockchain
// 3. send cross shard tx receipts to destination shard
func (node *Node) PostConsensusProcessingDIY(newBlock *types.Block) error {
	if node.Consensus.IsLeader() {
		// if node.NodeConfig.ShardID == shard.BeaconChainShardID {
		// 	node.BroadcastNewBlock(newBlock)
		// }
		node.BroadcastCXReceipts(newBlock)
	} else {
		if node.Consensus.Mode() != consensus.Listening {
			utils.Logger().Info().
				Uint64("blockNum", newBlock.NumberU64()).
				Uint64("epochNum", newBlock.Epoch().Uint64()).
				Uint64("ViewId", newBlock.Header().ViewID().Uint64()).
				Str("blockHash", newBlock.Hash().String()).
				Int("numTxns", len(newBlock.Transactions())).
				Int("numStakingTxns", len(newBlock.StakingTransactions())).
				Uint32("numSignatures", node.Consensus.NumSignaturesIncludedInBlock(newBlock)).
				Msg("BINGO !!! Reached Consensus")

			numSig := float64(node.Consensus.NumSignaturesIncludedInBlock(newBlock))
			node.Consensus.UpdateValidatorMetrics(numSig, float64(newBlock.NumberU64()))

			// // 1% of the validator also need to do broadcasting
			// rand.Seed(time.Now().UTC().UnixNano())
			// rnd := rand.Intn(100)
			// if rnd < 1 {
			// // Beacon validators also broadcast new blocks to make sure beacon sync is strong.
			// if node.NodeConfig.ShardID == shard.BeaconChainShardID {
			// 	node.BroadcastNewBlock(newBlock)
			// }
			node.BroadcastCXReceipts(newBlock)
			// }
		}
	}

	// Broadcast client requested missing cross shard receipts if there is any
	node.BroadcastMissingCXReceipts()

	if h := node.NodeConfig.WebHooks.Hooks; h != nil {
		if h.Availability != nil {
			for _, addr := range node.GetAddresses(newBlock.Epoch()) {
				wrapper, err := node.Beaconchain().ReadValidatorInformation(addr)
				if err != nil {
					utils.Logger().Err(err).Str("addr", addr.Hex()).Msg("failed reaching validator info")
					return nil
				}
				snapshot, err := node.Beaconchain().ReadValidatorSnapshot(addr)
				if err != nil {
					utils.Logger().Err(err).Str("addr", addr.Hex()).Msg("failed reaching validator snapshot")
					return nil
				}
				computed := availability.ComputeCurrentSigning(
					snapshot.Validator, wrapper,
				)
				lastBlockOfEpoch := shard.Schedule.EpochLastBlock(node.Beaconchain().CurrentBlock().Header().Epoch().Uint64())

				computed.BlocksLeftInEpoch = lastBlockOfEpoch - node.Beaconchain().CurrentBlock().Header().Number().Uint64()

				if err != nil && computed.IsBelowThreshold {
					url := h.Availability.OnDroppedBelowThreshold
					go func() {
						webhooks.DoPost(url, computed)
					}()
				}
			}
		}
	}
	return nil
}
