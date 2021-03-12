package rpc

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/core/types"
	common2 "github.com/harmony-one/harmony/internal/common"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/pkg/errors"
)

// 好像没用
// SendRawTransactionDIY will add the signed transaction to the transaction pool.
// The sender is responsible for signing the transaction and using the correct nonce.
func (s *PublicPoolService) SendRawTransactionDIY(
	ctx context.Context, encodedTx hexutil.Bytes,
) (common.Hash, error) {
	// DOS prevention
	if len(encodedTx) >= types.MaxEncodedPoolTransactionSize {
		err := errors.Wrapf(core.ErrOversizedData, "encoded tx size: %d", len(encodedTx))
		return common.Hash{}, err
	}

	var tx *types.Transaction
	var txHash common.Hash

	if s.version == Eth {
		ethTx := new(types.EthTransaction)
		if err := rlp.DecodeBytes(encodedTx, ethTx); err != nil {
			return common.Hash{}, err
		}
		txHash = ethTx.Hash()
		tx = ethTx.ConvertToHmy()
	} else {
		tx = new(types.Transaction)
		if err := rlp.DecodeBytes(encodedTx, tx); err != nil {
			return common.Hash{}, err
		}
		txHash = tx.Hash()
	}

	// Verify chainID
	if err := s.verifyChainID(tx); err != nil {
		return common.Hash{}, err
	}

	// Submit transaction
	if err := s.hmy.SendTx(ctx, tx); err != nil {
		utils.Logger().Warn().Err(err).Msg("Could not submit transaction")
		return txHash, err
	}

	// Log submission
	if tx.To() == nil {
		signer := types.MakeSigner(s.hmy.ChainConfig(), s.hmy.CurrentBlock().Epoch())
		ethSigner := types.NewEIP155Signer(s.hmy.ChainConfig().EthCompatibleChainID)

		if tx.IsEthCompatible() {
			signer = ethSigner
		}
		from, err := types.Sender(signer, tx)
		if err != nil {
			return common.Hash{}, err
		}
		addr := crypto.CreateAddress(from, tx.Nonce())
		utils.Logger().Info().
			Str("fullhash", tx.Hash().Hex()).
			Str("hashByType", tx.HashByType().Hex()).
			Str("contract", common2.MustAddressToBech32(addr)).
			Msg("Submitted contract creation")
	} else {
		utils.Logger().Info().
			Str("fullhash", tx.Hash().Hex()).
			Str("hashByType", tx.HashByType().Hex()).
			Str("recipient", tx.To().Hex()).
			Interface("tx", tx).
			Msg("Submitted transaction")
	}

	// Response output is the same for all versions
	return txHash, nil
}
