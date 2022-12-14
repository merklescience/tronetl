package tron

import (
	"bytes"
	"encoding/json"
	"io"
	"math/big"
	"math/rand"
	"net/http"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

type TronClient struct {
	httpURI string
	jsonURI string
}

func chk(err error) {
	if err != nil {
		panic(err)
	}
}

func NewTronClient(providerURL string) *TronClient {
	return &TronClient{
		httpURI: providerURL + ":8090",
		jsonURI: providerURL + ":50545/jsonrpc",
	}
}

func (c *TronClient) GetJSONBlockByNumberWithTxs(number *big.Int) *JSONBlockWithTxs {
	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_getBlockByNumber",
		"params": []any{
			toBlockNumArg(number), true,
		},
		"id": rand.Int(),
	})
	chk(err)
	resp, err := http.Post(c.jsonURI, "application/json", bytes.NewBuffer(payload))
	chk(err)
	body, err := io.ReadAll(resp.Body)
	chk(err)

	var rpcResp JSONResponse
	var block JSONBlockWithTxs
	err = json.Unmarshal(body, &rpcResp)
	chk(err)
	err = json.Unmarshal(rpcResp.Result, &block)
	chk(err)

	return &block
}

func (c *TronClient) GetJSONBlockByNumberWithTxIDs(number *big.Int) *JSONBlockWithTxIDs {
	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_getBlockByNumber",
		"params": []any{
			toBlockNumArg(number), false,
		},
		"id": rand.Int(),
	})
	chk(err)
	resp, err := http.Post(c.jsonURI, "application/json", bytes.NewBuffer(payload))
	chk(err)
	body, err := io.ReadAll(resp.Body)
	chk(err)

	var rpcResp JSONResponse
	var block JSONBlockWithTxIDs
	err = json.Unmarshal(body, &rpcResp)
	chk(err)
	err = json.Unmarshal(rpcResp.Result, &block)
	chk(err)

	return &block
}

func (c *TronClient) GetHTTPBlockByNumber(number *big.Int) *HTTPBlock {
	url := c.httpURI + "/wallet/getblockbynum" // + "?visible=true"
	payload, err := json.Marshal(map[string]any{
		"num": number.Uint64(),
		// "visable": true,
	})
	chk(err)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	chk(err)
	body, err := io.ReadAll(resp.Body)
	chk(err)

	var block HTTPBlock
	err = json.Unmarshal(body, &block)
	chk(err)

	return &block
}

func (c *TronClient) GetTxInfosByNumber(number uint64) []HTTPTxInfo {
	url := c.httpURI + "/wallet/gettransactioninfobyblocknum" // + "?visible=true"
	payload, err := json.Marshal(map[string]any{
		"num": number,
		// "visable": true,
	})
	chk(err)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	chk(err)
	body, err := io.ReadAll(resp.Body)
	chk(err)

	var txInfos []HTTPTxInfo
	err = json.Unmarshal(body, &txInfos)
	chk(err)

	return txInfos
}

func (c *TronClient) GetAccount(address string) *HTTPAccount {
	url := c.httpURI + "/wallet/getaccount" // + "?visible=true"
	payload, err := json.Marshal(map[string]any{
		"address": address,
		// "visable": true,
	})
	chk(err)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	chk(err)
	body, err := io.ReadAll(resp.Body)
	chk(err)

	var acc HTTPAccount
	err = json.Unmarshal(body, &acc)
	chk(err)

	return &acc
}

func (c *TronClient) GetContract(address string) *HTTPContract {
	url := c.httpURI + "/wallet/getcontract" // + "?visible=true"
	payload, err := json.Marshal(map[string]any{
		"value": address,
		// "visable": true,
	})
	chk(err)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	chk(err)
	body, err := io.ReadAll(resp.Body)
	chk(err)

	var contract HTTPContract
	err = json.Unmarshal(body, &contract)
	chk(err)

	return &contract
}

func toBlockNumArg(number *big.Int) string {
	if number == nil {
		return "latest"
	}
	pending := big.NewInt(-1)
	if number.Cmp(pending) == 0 {
		return "pending"
	}
	return hexutil.EncodeBig(number)
}
