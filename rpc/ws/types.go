// Copyright 2021 github.com/gagliardetto
// This file has been modified by github.com/gagliardetto
//
// Copyright 2020 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ws

import (
	"encoding/json"
	stdjson "encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/gagliardetto/solana-go/rpc/jsonrpc"
)

type request struct {
	Version string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
	ID      uint64      `json:"id"`
}

type resp struct {
	ID     json.Number       `json:"id,omitempty"`
	Method string            `json:"method,omitempty"`
	Result json.RawMessage   `json:"result,omitempty"`
	Error  *jsonrpc.RPCError `json:"error,omitempty"`
	Params *subParams        `json:"params,omitempty"`
}

type subParams struct {
	Subscription json.Number `json:"subscription,omitempty"`
	Result       json.RawMessage
}

func (r *resp) resultInt() (uint64, bool) {
	if r.Result == nil {
		return 0, false
	}

	var res uint64
	if err := sonicAPI.Unmarshal(r.Result, &res); err != nil {
		return 0, false
	}

	return res, true
}

func newRequest(params []interface{}, method string, configuration map[string]interface{}, shortID bool) *request {
	if params != nil && configuration != nil {
		params = append(params, configuration)
	}

	ID := uint64(rand.Int31())

	return &request{
		Version: "2.0",
		Method:  method,
		Params:  params,
		ID:      ID,
	}
}

func (c *request) encode() ([]byte, error) {
	data, err := jsonn.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("encode request: json marshall: %w", err)
	}
	return data, nil
}

type response struct {
	Version string              `json:"jsonrpc"`
	Params  *params             `json:"params"`
	Error   *stdjson.RawMessage `json:"error"`
}

type params struct {
	Result       *stdjson.RawMessage `json:"result"`
	Subscription int                 `json:"subscription"`
}

type Options struct {
	HttpHeader       http.Header
	HandshakeTimeout time.Duration
	ShortID          bool // some RPC do not support int63/uint64 id, so need to enable it to rand a int31/uint32 id
}

var DefaultHandshakeTimeout = 45 * time.Second
