package tron

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/holiman/uint256"
)

type TronClient struct {
	httpURI string
	jsonURI string
	client  *http.Client
}

// Helper function to check errors and return them instead of panicking
func handleError(err error) error {
	if err != nil {
		return fmt.Errorf("tron client error: %w", err)
	}
	return nil
}

// Helper function to make HTTP requests with retries
func (c *TronClient) makeRequestWithRetry(url string, payload []byte, maxRetries int) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 1s, 2s, 4s, 8s
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second
			time.Sleep(backoff)
		}

		resp, err := c.client.Post(url, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			lastErr = fmt.Errorf("HTTP request failed: %w", err)
			continue
		}

		defer resp.Body.Close()

		// Check if response is successful
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("HTTP request failed with status: %d", resp.StatusCode)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			lastErr = fmt.Errorf("failed to read response body: %w", err)
			continue
		}

		// Validate that we got a complete response
		if len(body) == 0 {
			lastErr = fmt.Errorf("empty response body")
			continue
		}

		// Try to validate JSON structure (basic check)
		if !json.Valid(body) {
			lastErr = fmt.Errorf("invalid JSON response: %s", string(body))
			continue
		}

		return body, nil
	}

	return nil, fmt.Errorf("request failed after %d attempts, last error: %w", maxRetries+1, lastErr)
}

func NewTronClient(providerURL string) *TronClient {
	return &TronClient{
		httpURI: providerURL + "",
		jsonURI: providerURL + "/jsonrpc",
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *TronClient) GetJSONBlockByNumberWithTxs(number *big.Int) (*JSONBlockWithTxs, error) {
	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_getBlockByNumber",
		"params": []any{
			toBlockNumArg(number), true,
		},
		"id": rand.Int(),
	})
	if err != nil {
		return nil, handleError(err)
	}

	body, err := c.makeRequestWithRetry(c.jsonURI, payload, 3)
	if err != nil {
		return nil, err
	}

	var rpcResp JSONResponse
	var block JSONBlockWithTxs
	err = json.Unmarshal(body, &rpcResp)
	if err != nil {
		return nil, handleError(err)
	}

	if rpcResp.Result == nil {
		return nil, fmt.Errorf("no result in RPC response")
	}

	err = json.Unmarshal(rpcResp.Result, &block)
	if err != nil {
		return nil, handleError(err)
	}

	return &block, nil
}

func (c *TronClient) GetJSONBlockByNumberWithTxIDs(number *big.Int) (*JSONBlockWithTxIDs, error) {
	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_getBlockByNumber",
		"params": []any{
			toBlockNumArg(number), false,
		},
		"id": rand.Int(),
	})
	if err != nil {
		return nil, handleError(err)
	}

	body, err := c.makeRequestWithRetry(c.jsonURI, payload, 3)
	if err != nil {
		return nil, err
	}

	var rpcResp JSONResponse
	var block JSONBlockWithTxIDs
	err = json.Unmarshal(body, &rpcResp)
	if err != nil {
		return nil, handleError(err)
	}

	if rpcResp.Result == nil {
		return nil, fmt.Errorf("no result in RPC response")
	}

	err = json.Unmarshal(rpcResp.Result, &block)
	if err != nil {
		return nil, handleError(err)
	}

	return &block, nil
}

func (c *TronClient) GetHTTPBlockByNumber(number *big.Int) (*HTTPBlock, error) {
	url := c.httpURI + "/wallet/getblockbynum"
	payload, err := json.Marshal(map[string]any{
		"num": number.Uint64(),
	})
	if err != nil {
		return nil, handleError(err)
	}

	body, err := c.makeRequestWithRetry(url, payload, 3)
	if err != nil {
		return nil, err
	}

	var block HTTPBlock
	err = json.Unmarshal(body, &block)
	if err != nil {
		return nil, handleError(err)
	}

	return &block, nil
}

func (c *TronClient) GetTxInfosByNumber(number uint64) ([]HTTPTxInfo, error) {
	if number == 0 {
		return []HTTPTxInfo{}, nil // 0 height returns `{}` which is not a list
	}

	url := c.httpURI + "/wallet/gettransactioninfobyblocknum"
	payload, err := json.Marshal(map[string]any{
		"num": number,
	})
	if err != nil {
		return nil, handleError(err)
	}

	body, err := c.makeRequestWithRetry(url, payload, 3)
	if err != nil {
		return nil, err
	}

	var txInfos []HTTPTxInfo
	err = json.Unmarshal(body, &txInfos)
	if err != nil {
		return nil, handleError(err)
	}

	return txInfos, nil
}

func (c *TronClient) GetAccount(address string) (*HTTPAccount, error) {
	url := c.httpURI + "/wallet/getaccount"
	payload, err := json.Marshal(map[string]any{
		"address": address,
	})
	if err != nil {
		return nil, handleError(err)
	}

	body, err := c.makeRequestWithRetry(url, payload, 3)
	if err != nil {
		return nil, err
	}

	var acc HTTPAccount
	err = json.Unmarshal(body, &acc)
	if err != nil {
		return nil, handleError(err)
	}

	return &acc, nil
}

func (c *TronClient) GetContract(address string) (*HTTPContract, error) {
	url := c.httpURI + "/wallet/getcontract"
	payload, err := json.Marshal(map[string]any{
		"value": address,
	})
	if err != nil {
		return nil, handleError(err)
	}

	body, err := c.makeRequestWithRetry(url, payload, 3)
	if err != nil {
		return nil, err
	}

	var contract HTTPContract
	err = json.Unmarshal(body, &contract)
	if err != nil {
		return nil, handleError(err)
	}

	return &contract, nil
}

type CallResult struct {
	Result struct {
		Result  bool   `json:"result,omitempty"`
		Code    string `json:"code,omitempty"` // contains "ERROR" when is error
		Message string `json:"message,omitempty"`
	} `json:"result,omitempty"`
	EnergyUsed     int              `json:"energy_used"`
	ConstantResult []string         `json:"constant_result"`
	Transaction    *HTTPTransaction `json:"transaction"`
}

type Address string

// Call is offline
func (c *TronClient) CallContract(contractAddr, callerAddr string, val, feeLimit int64, funcSig string, params ...any) (*CallResult, error) {
	url := c.httpURI + "/wallet/triggerconstantcontract"
	u256Params := make([]string, len(params))
	for i, param := range params {
		switch p := param.(type) {
		case uint64:
			u256Params[i] = uint256.NewInt(p).Hex()[2:]
		case Address:
			addr := EnsureHexAddr(string(p))
			addr = addr[2:]

			u, err := uint256.FromHex(addr)
			if err != nil {
				return nil, fmt.Errorf("invalid address parameter: %w", err)
			}
			u256Params[i] = u.Hex()[2:]
		default:
			return nil, fmt.Errorf("unsupported type: %#+v", param)
		}
	}
	payload, err := json.Marshal(map[string]any{
		"contract_address":  contractAddr,
		"function_selector": funcSig,
		"parameter":         "",
		"fee_limit":         feeLimit,
		"call_value":        val,
		"owner_address":     callerAddr, // = caller
	})
	if err != nil {
		return nil, handleError(err)
	}

	body, err := c.makeRequestWithRetry(url, payload, 3)
	if err != nil {
		return nil, err
	}

	var result CallResult
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, handleError(err)
	}

	return &result, nil
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

func (c *TronClient) GetLatestBlock() (uint64, error) {
	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_blockNumber",
		"params":  []any{},
		"id":      rand.Int(),
	})
	if err != nil {
		return 0, handleError(err)
	}

	body, err := c.makeRequestWithRetry(c.jsonURI, payload, 3)
	if err != nil {
		return 0, err
	}

	var rpcResp JSONLatestBlock
	err = json.Unmarshal(body, &rpcResp)
	if err != nil {
		return 0, handleError(err)
	}

	if rpcResp.Result == "" {
		return 0, fmt.Errorf("no result in RPC response")
	}

	result, err := hexutil.DecodeUint64(rpcResp.Result)
	if err != nil {
		return 0, handleError(err)
	}
	return result, nil
}
