package main

import (
	"encoding/hex"
	"encoding/json"
	"log"
	"math/big"
	"strings"

	"git.ngx.fi/c0mm4nd/tronetl/tron"
	"golang.org/x/crypto/sha3"
)

// CsvTransaction represents a tron tx csv output, not trc10
// 1 TRX = 1000000 sun
type CsvTransaction struct {
	Hash             string `csv:"hash" json:"hash"`
	Nonce            string `csv:"nonce" json:"nonce"`
	BlockHash        string `csv:"block_hash" json:"block_hash"`
	BlockNumber      uint64 `csv:"block_number" json:"block_number"`
	TransactionIndex int    `csv:"transaction_index" json:"transaction_index"`

	FromAddress          string `csv:"from_address" json:"from_address"`
	ToAddress            string `csv:"to_address" json:"to_address"`
	Value                string `csv:"value" json:"value"`
	Gas                  string `csv:"gas" json:"gas"`
	GasPrice             string `csv:"gas_price" json:"gas_price"`
	Input                string `csv:"input" json:"input"`
	BlockTimestamp       uint64 `csv:"block_timestamp" json:"block_timestamp"`
	MaxFeePerGas         string `csv:"max_fee_per_gas" json:"max_fee_per_gas"`
	MaxPriorityFeePerGas string `csv:"max_priority_fee_per_gas" json:"max_priority_fee_per_gas"`
	TransactionType      string `csv:"transaction_type" json:"transaction_type"`

	Status string `csv:"status" json:"status"`

	// appendix
	TransactionTimestamp  int64 `csv:"transaction_timestamp" json:"transaction_timestamp"`
	TransactionExpiration int64 `csv:"transaction_expiration" json:"transaction_expiration"`
	FeeLimit              int64 `csv:"fee_limit" json:"fee_limit"`
}

// NewCsvTransaction creates a new CsvTransaction
func NewCsvTransaction(blockTimestamp uint64, txIndex int, jsontx *tron.JSONTransaction, httptx *tron.HTTPTransaction) *CsvTransaction {

	to := ""
	if jsontx.To != "" {
		to = tron.EnsureTAddr(jsontx.To[2:])
	}

	from := ""
	if jsontx.From != "" {
		from = tron.EnsureTAddr(jsontx.From[2:])
	}

	txType := "Unknown"
	if len(httptx.RawData.Contract) > 0 {
		txType = httptx.RawData.Contract[0].ContractType
	}

	status := ""
	if len(httptx.Ret) > 0 {
		status = httptx.Ret[0].ContractRet
	}

	return &CsvTransaction{
		Hash:                 jsontx.Hash[2:],
		Nonce:                jsontx.Nonce,
		BlockHash:            jsontx.BlockHash[2:],
		BlockNumber:          uint64(*jsontx.BlockNumber),
		TransactionIndex:     txIndex,
		FromAddress:          from, //tron.EnsureTAddr(jsontx.From[2:]),
		ToAddress:            to,
		Value:                jsontx.Value.ToInt().String(),
		Gas:                  jsontx.Gas.ToInt().String(),
		GasPrice:             jsontx.GasPrice.ToInt().String(), // https://support.ledger.com/hc/en-us/articles/6331588714141-How-do-Tron-TRX-fees-work-?support=true
		Input:                jsontx.Input[2:],
		BlockTimestamp:       blockTimestamp / 1000, // unit: sec
		MaxFeePerGas:         "",                    //tx.MaxFeePerGas.String(),
		MaxPriorityFeePerGas: "",                    //tx.MaxPriorityFeePerGas.String(),
		TransactionType:      txType,                //jsontx.Type[2:],

		Status: status, // can be SUCCESS REVERT

		// appendix
		TransactionTimestamp:  httptx.RawData.Timestamp / 1000,  // float64(httptx.RawData.Timestamp) * 1 / 1000,
		TransactionExpiration: httptx.RawData.Expiration / 1000, // float64(httptx.RawData.Expiration) * 1 / 1000,
		FeeLimit:              httptx.RawData.FeeLimit,
	}
}

// CsvBlock represents a tron block output
type CsvBlock struct {
	Number           uint64 `csv:"number" json:"number"`
	Hash             string `csv:"hash" json:"hash"`
	ParentHash       string `csv:"parent_hash" json:"parent_hash"`
	Nonce            string `csv:"nonce" json:"nonce"`
	Sha3Uncles       string `csv:"sha3_uncles" json:"sha3_uncles"`
	LogsBloom        string `csv:"logs_bloom" json:"logs_bloom"`
	TransactionsRoot string `csv:"transaction_root" json:"transactions_root"`
	StateRoot        string `csv:"state_root" json:"state_root"`
	ReceiptsRoot     string `csv:"receipts_root" json:"receipts_root"`
	Miner            string `csv:"miner" json:"miner"`
	Difficulty       string `csv:"difficulty" json:"difficulty"`
	TotalDifficulty  string `csv:"total_difficulty" json:"total_difficulty"`
	Size             uint64 `csv:"size" json:"size"`
	ExtraData        string `csv:"extra_data" json:"extra_data"`
	GasLimit         string `csv:"gas_limit" json:"gas_limit"`
	GasUsed          string `csv:"gas_used" json:"gas_used"`
	Timestamp        uint64 `csv:"timestamp" json:"timestamp"`
	TansactionCount  int    `csv:"transaction_count" json:"transaction_count"`
	BaseFeePerGas    string `csv:"base_fee_per_gas" json:"base_fee_per_gas"`

	// append
	WitnessSignature string `csv:"witness_signature" json:"witness_signature"`
}

// NewCsvBlock creates a new CsvBlock
func NewCsvBlock(jsonblock *tron.JSONBlockWithTxs, httpblock *tron.HTTPBlock) *CsvBlock {
	return &CsvBlock{
		Number:           uint64(*jsonblock.Number),
		Hash:             jsonblock.Hash[2:],
		ParentHash:       jsonblock.ParentHash[2:],
		Nonce:            jsonblock.Nonce,
		Sha3Uncles:       "", // block.Sha3Uncles,
		LogsBloom:        jsonblock.LogsBloom[2:],
		TransactionsRoot: jsonblock.TransactionsRoot[2:],
		StateRoot:        jsonblock.StateRoot[2:],
		ReceiptsRoot:     "",                                    // block.ReceiptsRoot
		Miner:            tron.EnsureTAddr(jsonblock.Miner[2:]), // = WitnessAddress
		Difficulty:       "",
		TotalDifficulty:  "",
		Size:             uint64(*jsonblock.Size),
		ExtraData:        "",
		GasLimit:         jsonblock.GasLimit.ToInt().String(),
		GasUsed:          jsonblock.GasUsed.ToInt().String(),
		Timestamp:        uint64(httpblock.BlockHeader.RawData.Timestamp) / 1000,
		TansactionCount:  len(jsonblock.Transactions),
		BaseFeePerGas:    "", // block.BaseFeePerGas,

		//append
		WitnessSignature: httpblock.BlockHeader.WitnessSignature,
	}
}

// CsvTRC10Transfer is a trc10 transfer output
// https://developers.tron.network/docs/trc10-transfer-in-smart-contracts
// https://tronprotocol.github.io/documentation-en/mechanism-algorithm/system-contracts/
// It represents:
// - TransferContract
// - TransferAssetContract
type CsvTRC10Transfer struct {
	BlockNumber       uint64 `csv:"block_number" json:"block_number"`
	BlockHash         string `csv:"block_hash" json:"block_hash"`
	TransactionHash   string `csv:"transaction_hash" json:"transaction_hash"`
	TransactionIndex  int    `csv:"transaction_index" json:"transaction_index"`
	ContractCallIndex int    `csv:"contract_call_index" json:"contract_call_index"`

	AssetName      string `csv:"asset_name" json:"asset_name"` // do not omit => empty means trx
	FromAddress    string `csv:"from_address" json:"from_address"`
	ToAddress      string `csv:"to_address" json:"to_address"`
	Value          string `csv:"value" json:"value"`
	BlockTimestamp uint64 `csv:"block_timestamp" json:"block_timestamp"`
}

// NewCsvTRC10Transfer creates a new CsvTRC10Transfer
func NewCsvTRC10Transfer(blockHash string, blockNum uint64, txIndex, callIndex int, httpTx *tron.HTTPTransaction, tfParams *tron.TRC10TransferParams, blockTimestamp uint64) *CsvTRC10Transfer {
	asset, _ := hex.DecodeString(tfParams.AssetName)
	return &CsvTRC10Transfer{
		TransactionHash:   httpTx.TxID,
		BlockHash:         blockHash,
		BlockNumber:       blockNum,
		TransactionIndex:  txIndex,
		ContractCallIndex: callIndex,

		AssetName:      string(asset),
		FromAddress:    tron.EnsureTAddr(tfParams.OwnerAddress),
		ToAddress:      tron.EnsureTAddr(tfParams.ToAddress),
		Value:          tfParams.Amount.String(),
		BlockTimestamp: blockTimestamp,
	}
}

// CsvLog is a EVM smart contract event log output
type CsvLog struct {
	BlockNumber     uint64 `csv:"block_number" json:"block_number"`
	TransactionHash string `csv:"transaction_hash" json:"transaction_hash"`
	LogIndex        uint   `csv:"log_index" json:"log_index"`

	Address string `csv:"address" json:"address"`
	Topics  string `csv:"topics" json:"topics"`
	Data    string `csv:"data" json:"data"`
}

// NewCsvLog creates a new CsvLog
func NewCsvLog(blockNumber uint64, txHash string, logIndex uint, log *tron.HTTPTxInfoLog) *CsvLog {
	return &CsvLog{
		BlockNumber:     blockNumber,
		TransactionHash: txHash,
		LogIndex:        logIndex,

		Address: tron.EnsureTAddr(log.Address),
		Topics:  strings.Join(log.Topics, ";"),
		Data:    log.Data,
	}
}

// CsvInternalTx is a EVM smart contract internal transaction
type CsvInternalTx struct {
	BlockNumber             uint64 `csv:"block_number" json:"block_number"`
	TransactionHash         string `csv:"transaction_hash" json:"transaction_hash"`
	Index                   uint   `csv:"internal_index" json:"internal_index"`
	InternalTransactionHash string `csv:"internal_hash" json:"internal_hash"`
	CallerAddress           string `csv:"caller_address" json:"caller_address"`
	TransferToAddress       string `csv:"transferTo_address" json:"transferTo_address"`
	CallInfoIndex           uint   `csv:"call_info_index" json:"call_info_index"`
	CallTokenID             string `csv:"call_token_id" json:"call_token_id"`
	CallValue               int64  `csv:"call_value" json:"call_value"`
	Note                    string `csv:"note" json:"note"`
	Rejected                bool   `csv:"rejected" json:"rejected"`
	TokenAddress            string `csv:"token_address" json:"token_address"`
	BlockTimestamp          uint64 `csv:"block_timestamp" json:"block_timestamp"`
}

// NewCsvInternalTx creates a new CsvInternalTx
func NewCsvInternalTx(blockNum uint64, txHash string, index uint, itx *tron.HTTPInternalTransaction, callInfoIndex uint, tokenID string, value int64, blockTimestamp uint64) *CsvInternalTx {

	return &CsvInternalTx{
		BlockNumber:             blockNum,
		TransactionHash:         txHash,
		Index:                   index,
		InternalTransactionHash: itx.InternalTransactionHash,
		CallerAddress:           tron.EnsureTAddr(itx.CallerAddress),
		TransferToAddress:       tron.EnsureTAddr(itx.TransferToAddress),
		// CallValueInfo:     strings.Join(callValues, ";"),
		CallInfoIndex: callInfoIndex,
		CallTokenID:   tokenID,
		CallValue:     value,

		Note:           itx.Note,
		Rejected:       itx.Rejected,
		TokenAddress:   "0x0000",
		BlockTimestamp: blockTimestamp,
	}
}

// CsvReceipt is a receipt for tron transaction
type CsvReceipt struct {
	TxHash  string `csv:"transaction_hash" json:"transaction_hash"`
	TxIndex uint   `csv:"transaction_index" json:"transaction_index"`
	// BlockHash         string `csv:"block_hash"` // cannot get this
	BlockNumber       uint64 `csv:"block_number" json:"block_number"`
	ContractAddress   string `csv:"contract_address" json:"contract_address"`
	EnergyUsage       int64  `csv:"energy_usage,omitempty" json:"energy_usage"`
	EnergyFee         int64  `csv:"energy_fee,omitempty" json:"energy_fee"`
	OriginEnergyUsage int64  `csv:"origin_energy_usage,omitempty" json:"origin_energy_usage"`
	EnergyUsageTotal  int64  `csv:"energy_usage_total,omitempty" json:"energy_usage_total"`
	NetUsage          int64  `csv:"net_usage,omitempty" json:"net_usage"`
	NetFee            int64  `csv:"net_fee,omitempty" json:"net_fee"`
	Result            string `csv:"result" json:"result"`
	Fee               int64  `csv:"fee" json:"fee"`
}

func NewCsvReceipt(blockNum uint64, txHash string, txIndex uint, contractAddr string, tx_fee int64, r *tron.HTTPReceipt) *CsvReceipt {

	return &CsvReceipt{
		TxHash:  txHash,
		TxIndex: txIndex,
		// BlockHash:         blockHash,
		BlockNumber:       blockNum,
		ContractAddress:   contractAddr,
		EnergyUsage:       r.EnergyUsage,
		EnergyFee:         r.EnergyFee,
		OriginEnergyUsage: r.OriginEnergyUsage,
		EnergyUsageTotal:  r.EnergyUsageTotal,
		NetUsage:          r.NetUsage,
		NetFee:            r.NetFee,
		Result:            r.Result,
		Fee:               tx_fee,
	}
}

// CsvAccount is a tron account
type CsvAccount struct {
	AccountName string `csv:"account_name" json:"account_name"`
	Address     string `csv:"address" json:"address"`
	Type        string `csv:"type" json:"type"`
	CreateTime  int64  `csv:"create_time" json:"create_time"`

	// DecodedName string `csv:decoded_name`
}

func NewCsvAccount(acc *tron.HTTPAccount) *CsvAccount {
	name, _ := hex.DecodeString(acc.AccountName)
	return &CsvAccount{
		AccountName: string(name),
		Address:     tron.EnsureTAddr(acc.Address),
		Type:        acc.AccountType,
		CreateTime:  acc.CreateTime / 1000,
	}
}

// CsvContract is a standard EVM contract
type CsvContract struct {
	Address           string `csv:"address"`
	Bytecode          string `csv:"bytecode"`
	FunctionSighashes string `csv:"function_sighashes"`
	IsErc20           bool   `csv:"is_erc20"`
	IsErc721          bool   `csv:"is_erc721"`
	BlockNumber       uint64 `csv:"block_number"`

	// append some...
	ContractName               string
	ConsumeUserResourcePercent int
	OriginAddress              string
	OriginEnergyLimit          int64
}

var keccakHasher = sha3.NewLegacyKeccak256()

func NewCsvContract(c *tron.HTTPContract) *CsvContract {
	hashes := make([]string, 0, len(c.Abi.Entrys))
	for _, abi := range c.Abi.Entrys {
		if strings.ToLower(abi.Type) == "function" {
			content := abi.Name + "("
			types := make([]string, 0, len(abi.Inputs))
			for _, input := range abi.Inputs {
				types = append(types, input.Type)
			}
			funchash := keccakHasher.Sum([]byte(content + strings.Join(types, ",") + ")"))
			hashes = append(hashes, hex.EncodeToString(funchash))
		}
	}

	isErc20 := implementsAnyOf(hashes, "totalSupply()") &&
		implementsAnyOf(hashes, "balanceOf(address)") &&
		implementsAnyOf(hashes, "transfer(address,uint256)") &&
		implementsAnyOf(hashes, "transferFrom(address,address,uint256)") &&
		implementsAnyOf(hashes, "approve(address,uint256)") &&
		implementsAnyOf(hashes, "allowance(address,address)")

	isErc721 := implementsAnyOf(hashes, "balanceOf(address)") &&
		implementsAnyOf(hashes, "ownerOf(uint256)") &&
		implementsAnyOf(hashes, "transfer(address,uint256)", "transferFrom(address,address,uint256)") &&
		implementsAnyOf(hashes, "approve(address,uint256)")

	return &CsvContract{
		Address:           tron.EnsureTAddr(c.ContractAddress),
		Bytecode:          c.Bytecode,
		FunctionSighashes: strings.Join(hashes, ";"),
		IsErc20:           isErc20,
		IsErc721:          isErc721,
		BlockNumber:       0,

		// append
		ContractName:               c.Name,
		ConsumeUserResourcePercent: c.ConsumeUserResourcePercent,
		OriginAddress:              tron.EnsureTAddr(c.OriginAddress),
		OriginEnergyLimit:          c.OriginEnergyLimit,
	}
}

func implementsAnyOf(hashes []string, sigStrs ...string) bool {
	for i := range sigStrs {
		hash := hex.EncodeToString(keccakHasher.Sum([]byte(sigStrs[i])))
		for j := range hashes {
			if hashes[j] == hash {
				return true
			}
		}
	}

	return false
}

// CsvTokens is a standard EVM contract token
type CsvTokens struct {
	Address     string `csv:"address" json:"address"`
	Symbol      string `csv:"symbol" json:"symbol"`
	Name        string `csv:"name" json:"name"`
	Decimals    uint64 `csv:"decimals" json:"decimals"`
	TotalSupply uint64 `csv:"total_supply" json:"total_supply"`
	BlockNumber uint64 `csv:"block_number" json:"block_number"`
}

func NewCsvTokens(cli *tron.TronClient, contract *tron.HTTPContract) *CsvTokens {
	contractAddr := contract.ContractAddress
	callerAddr := contract.OriginAddress

	symbolResult := cli.CallContract(contractAddr, callerAddr, 0, 1000,
		"symbol()",
	)
	symbol := ParseSymbol(symbolResult.ConstantResult)
	if symbol == nil {
		log.Println("failed to parse symbol for contract", tron.EnsureHexAddr(contractAddr))
		result, _ := json.Marshal(symbolResult)
		log.Println(string(result))
		return nil
	}

	nameResult := cli.CallContract(contractAddr, callerAddr, 0, 1000,
		"name()",
	)
	name := ParseName(nameResult.ConstantResult)
	if name == nil {
		log.Println("failed to parse name for contract", tron.EnsureHexAddr(contractAddr))
		result, _ := json.Marshal(nameResult)
		log.Println(string(result))
		return nil
	}

	decimalsResult := cli.CallContract(contractAddr, callerAddr, 0, 1000,
		"decimals()",
	)
	decimals := ParseDecimals(decimalsResult.ConstantResult)
	if decimals == nil {
		log.Println("failed to parse decimals for contract", tron.EnsureHexAddr(contractAddr))
		result, _ := json.Marshal(decimalsResult)
		log.Println(string(result))
		return nil
	}

	totalSupplyResult := cli.CallContract(contractAddr, callerAddr, 0, 1000,
		"totalSupply()",
	)
	totalSupply := ParseTotalSupply(totalSupplyResult.ConstantResult)
	if totalSupply == nil {
		log.Println("failed to parse totalSupply for contract", tron.EnsureHexAddr(contractAddr))
		result, _ := json.Marshal(totalSupplyResult)
		log.Println(string(result))
		return nil
	}

	block := cli.GetJSONBlockByNumberWithTxIDs(nil)

	return &CsvTokens{
		Address:     contractAddr,
		Symbol:      *symbol,
		Name:        *name,
		Decimals:    *decimals,
		TotalSupply: *totalSupply,
		BlockNumber: uint64(*block.Number),
	}
}

type StreamCsvTransactionReceipt struct {
	Hash             string `csv:"hash" json:"hash"`
	Nonce            string `csv:"nonce" json:"nonce"`
	BlockHash        string `csv:"block_hash" json:"block_hash"`
	BlockNumber      uint64 `csv:"block_number" json:"block_number"`
	TransactionIndex int    `csv:"transaction_index" json:"transaction_index"`

	FromAddress          string `csv:"from_address" json:"from_address"`
	ToAddress            string `csv:"to_address" json:"to_address"`
	Value                string `csv:"value" json:"value"`
	Gas                  string `csv:"gas" json:"gas"`
	GasPrice             string `csv:"gas_price" json:"gas_price"`
	Input                string `csv:"input" json:"input"`
	BlockTimestamp       uint64 `csv:"block_timestamp" json:"block_timestamp"`
	MaxFeePerGas         string `csv:"max_fee_per_gas" json:"max_fee_per_gas"`
	MaxPriorityFeePerGas string `csv:"max_priority_fee_per_gas" json:"max_priority_fee_per_gas"`
	TransactionType      string `csv:"transaction_type" json:"transaction_type"`

	Status string `csv:"status" json:"status"`

	// appendix
	TransactionTimestamp  int64 `csv:"transaction_timestamp" json:"transaction_timestamp"`
	TransactionExpiration int64 `csv:"transaction_expiration" json:"transaction_expiration"`
	FeeLimit              int64 `csv:"fee_limit" json:"fee_limit"`

	ContractAddress   string `csv:"receipts_contract_address" json:"receipts_contract_address"`
	EnergyUsage       int64  `csv:"receipts_energy_usage,omitempty" json:"receipts_energy_usage"`
	EnergyFee         int64  `csv:"receipts_energy_fee,omitempty" json:"receipts_energy_fee"`
	OriginEnergyUsage int64  `csv:"receipts_origin_energy_usage,omitempty" json:"receipts_origin_energy_usage"`
	EnergyUsageTotal  int64  `csv:"receipts_energy_usage_total,omitempty" json:"receipts_energy_usage_total"`
	NetUsage          int64  `csv:"receipts_net_usage,omitempty" json:"receipts_net_usage"`
	NetFee            int64  `csv:"receipts_net_fee,omitempty" json:"receipts_net_fee"`
	Result            string `csv:"receipts_result" json:"receipts_result"`
	Fee               int64  `csv:"receipts_fee" json:"receipts_fee"`
	TokenAddress      string `csv:"token_address" json:"token_address"`
}

func NewStreamCsvTransactionReceipt(blockNum uint64, txHash string, txIndex uint, contractAddr string, tx_fee int64, r *tron.HTTPReceipt, jsontx *CsvTransaction) *StreamCsvTransactionReceipt {
	return &StreamCsvTransactionReceipt{
		Hash:                 jsontx.Hash,
		Nonce:                jsontx.Nonce,
		BlockHash:            jsontx.BlockHash,
		BlockNumber:          jsontx.BlockNumber,
		TransactionIndex:     jsontx.TransactionIndex,
		FromAddress:          jsontx.FromAddress, //tron.EnsureTAddr(jsontx.From[2:]),
		ToAddress:            jsontx.ToAddress,
		Value:                jsontx.Value,
		Gas:                  jsontx.Gas,
		GasPrice:             jsontx.GasPrice, // https://support.ledger.com/hc/en-us/articles/6331588714141-How-do-Tron-TRX-fees-work-?support=true
		Input:                jsontx.Input,
		BlockTimestamp:       jsontx.BlockTimestamp,  // unit: sec
		MaxFeePerGas:         "",                     //tx.MaxFeePerGas.String(),
		MaxPriorityFeePerGas: "",                     //tx.MaxPriorityFeePerGas.String(),
		TransactionType:      jsontx.TransactionType, //jsontx.Type[2:],

		Status: jsontx.Status, // can be SUCCESS REVERT

		// appendix
		TransactionTimestamp:  jsontx.TransactionTimestamp,  // float64(httptx.RawData.Timestamp) * 1 / 1000,
		TransactionExpiration: jsontx.TransactionExpiration, // float64(httptx.RawData.Expiration) * 1 / 1000,
		FeeLimit:              jsontx.FeeLimit,

		ContractAddress:   contractAddr,
		EnergyUsage:       r.EnergyUsage,
		EnergyFee:         r.EnergyFee,
		OriginEnergyUsage: r.OriginEnergyUsage,
		EnergyUsageTotal:  r.EnergyUsageTotal,
		NetUsage:          r.NetUsage,
		NetFee:            r.NetFee,
		Result:            r.Result,
		Fee:               tx_fee,
		TokenAddress:      "0x0000",
	}
}

func ParseSymbol(contractResults []string) *string {
	if len(contractResults) == 0 {
		return nil
	}

	result := contractResults[0]
	bigLlen, ok := new(big.Int).SetString(result[0:64], 16)
	if !ok {
		// TODO: warn log here
		return nil
	}
	l, ok := new(big.Int).SetString(result[64:64+bigLlen.Int64()*2], 16)
	if !ok {
		// TODO: warn log here
		return nil
	}
	hexStr := result[64+bigLlen.Int64()*2 : 64+bigLlen.Int64()*2+l.Int64()*2]
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		// TODO: err log here
		return nil
	}
	rtn := string(decoded)
	return &rtn
}

func ParseName(contractResults []string) *string {
	if len(contractResults) == 0 {
		return nil
	}

	result := contractResults[0]
	bigLlen, ok := new(big.Int).SetString(result[0:64], 16)
	if !ok {
		// TODO: warn log here
		return nil
	}
	l, ok := new(big.Int).SetString(result[64:64+bigLlen.Int64()*2], 16)
	if !ok {
		// TODO: warn log here
		return nil
	}
	hexStr := result[64+bigLlen.Int64()*2 : 64+bigLlen.Int64()*2+l.Int64()*2]
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		// TODO: err log here
		return nil
	}

	rtn := string(decoded)
	return &rtn
}

func ParseDecimals(contractResults []string) *uint64 {
	if len(contractResults) == 0 {
		panic("failed to parse symbol")
	}

	result, ok := new(big.Int).SetString(contractResults[0], 16)
	if !ok {
		// TODO: err log here
		return nil
	}

	rtn := result.Uint64()
	return &rtn
}

func ParseTotalSupply(contractResults []string) *uint64 {
	if len(contractResults) == 0 {
		panic("failed to parse symbol")
	}

	result, ok := new(big.Int).SetString(contractResults[0], 16)
	if !ok {
		// TODO: err log here
		return nil
	}

	rtn := result.Uint64()
	return &rtn
}
