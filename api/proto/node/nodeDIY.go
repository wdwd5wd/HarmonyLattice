package node

import (
	"bytes"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/internal/utils"
)

// ConstructCXContract constructs cross shard contracts
func ConstructCXContractDIY(cxContract *types.CXContract) []byte {
	byteBuffer := bytes.NewBuffer(cxcontractH)
	by, err := rlp.EncodeToBytes(cxContract)
	if err != nil {
		const msg = "[ConstructCXContract] Encode ConstructCXContract Error"
		utils.Logger().Error().Err(err).Msg(msg)
		return []byte{}
	}
	byteBuffer.Write(by)
	return byteBuffer.Bytes()
}

// CallCXContract call another smart contract
func CallCXContractDIY(cxContract *types.CXContract) []byte {
	byteBuffer := bytes.NewBuffer(callcontractH)
	by, err := rlp.EncodeToBytes(cxContract)
	if err != nil {
		const msg = "[CallCXContract] Encode ConstructCXResult Error"
		utils.Logger().Error().Err(err).Msg(msg)
		return []byte{}
	}
	byteBuffer.Write(by)
	return byteBuffer.Bytes()
}

// ConstructCXResult constructs cross shard contract results
func ConstructCXResultDIY(cxContract *types.CXContract) []byte {
	byteBuffer := bytes.NewBuffer(cxresultH)
	by, err := rlp.EncodeToBytes(cxContract)
	if err != nil {
		const msg = "[ConstructCXResult] Encode ConstructCXResult Error"
		utils.Logger().Error().Err(err).Msg(msg)
		return []byte{}
	}
	byteBuffer.Write(by)
	return byteBuffer.Bytes()
}
