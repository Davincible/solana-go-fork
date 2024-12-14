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
	"bytes"
	"context"
	jjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/Davincible/d-utils/safemap"
	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/bytebufferpool"
	"golang.org/x/exp/slog"

	"github.com/gagliardetto/solana-go/utils/completion"
)

var (
	bufferPool = bytebufferpool.Pool{}
	jsonn      = jsoniter.ConfigCompatibleWithStandardLibrary

	// Configure Sonic for fastest performance
	sonicAPI = sonic.ConfigFastest

	// Pool for message type structs
	messageTypePool = sync.Pool{
		New: func() any {
			return &resp{}
		},
	}

	// Pool for raw message buffers
	rawMessagePool = sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, 10*1024*1024)) // 10MB initial capacity
		},
	}
)

var ErrSubscriptionClosed = errors.New("subscription closed")

type result any

type messageTask struct {
	buf *bytes.Buffer
}

type Client struct {
	rpcURL                  string
	conn                    *websocket.Conn
	connCtx                 context.Context
	connCtxCancel           context.CancelFunc
	lock                    sync.RWMutex
	subscriptionByRequestID safemap.Map[*Subscription]
	subscriptionByWSSubID   safemap.Map[*Subscription]
	reconnectOnErr          bool
	shortID                 bool

	pong *completion.Completer

	// Worker pool
	workerCount int
	taskChan    chan messageTask
}

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second
	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second
	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Default number of workers
	defaultWorkers = 4
)

func Connect(ctx context.Context, rpcEndpoint string) (c *Client, err error) {
	return ConnectWithOptions(ctx, rpcEndpoint, nil)
}

func ConnectWithOptions(ctx context.Context, rpcEndpoint string, opt *Options) (c *Client, err error) {
	workerCount := defaultWorkers
	if runtime.NumCPU() > 2 {
		workerCount = runtime.NumCPU() - 1
	}

	c = &Client{
		rpcURL:                  rpcEndpoint,
		subscriptionByRequestID: safemap.NewSafeMap[*Subscription](),
		subscriptionByWSSubID:   safemap.NewSafeMap[*Subscription](),
		pong:                    completion.New(),
		reconnectOnErr:          true,
		workerCount:             workerCount,
		taskChan:                make(chan messageTask, 1),
	}

	dialer := &websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  DefaultHandshakeTimeout,
		EnableCompression: false,            // Disable compression for large messages
		ReadBufferSize:    10 * 1024 * 1024, // 10MB read buffer
		WriteBufferSize:   1024 * 1024,      // 1MB write buffer
	}

	if opt != nil && opt.ShortID {
		c.shortID = opt.ShortID
	}

	if opt != nil && opt.HandshakeTimeout > 0 {
		dialer.HandshakeTimeout = opt.HandshakeTimeout
	}

	var httpHeader http.Header = nil
	if opt != nil && opt.HttpHeader != nil && len(opt.HttpHeader) > 0 {
		httpHeader = opt.HttpHeader
	}

	var resp *http.Response
	c.conn, resp, err = dialer.DialContext(ctx, rpcEndpoint, httpHeader)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(resp.Body)
			err = fmt.Errorf("new ws client: dial: %w, status: %s, body: %q", err, resp.Status, string(body))
		} else {
			err = fmt.Errorf("new ws client: dial: %w", err)
		}
		return nil, err
	}

	err = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	if err != nil {
		return nil, fmt.Errorf("failed to set read deadline: %w", err)
	}

	c.connCtx, c.connCtxCancel = context.WithCancel(context.Background())

	// Start workers
	for i := 0; i < c.workerCount; i++ {
		go c.messageWorker()
	}

	// Start connection maintenance goroutine
	go c.maintainConnection(c.connCtx)

	// Start ping/pong handler
	go func() {
		c.conn.SetPongHandler(func(string) error {
			if c.conn == nil || c.pong == nil {
				return nil
			}

			c.conn.SetReadDeadline(time.Now().Add(pongWait))
			c.pong.Complete()
			c.pong.Reset()
			return nil
		})
		c.conn.SetPingHandler(func(string) error {
			return c.sendPong()
		})

		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-c.connCtx.Done():
				return
			case <-ticker.C:
				if err := c.sendPing(); err != nil {
					slog.Error("failed to send ping", "err", err.Error())
					return
				}
			}
		}
	}()

	go c.receiveMessages()
	return c, nil
}

func (c *Client) messageWorker() {
	for {
		select {
		case <-c.connCtx.Done():
			return
		case task := <-c.taskChan:
			c.processMessage(task.buf)
		}
	}
}

func (c *Client) maintainConnection(ctx context.Context) {
	backoff := time.Second
	maxBackoff := time.Minute * 2

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			if c.conn == nil {
				if err := c.Connect(); err != nil {
					slog.Error("reconnection failed", "err", err.Error())
					backoff = time.Duration(math.Min(float64(backoff*2), float64(maxBackoff)))
					continue
				}
				backoff = time.Second
				slog.Info("successfully reconnected to websocket")
			}
		}
	}
}

func (c *Client) Connect() error {
	dialer := &websocket.Dialer{
		ReadBufferSize:    8 * 1024 * 1024,
		WriteBufferSize:   1024 * 1024,
		HandshakeTimeout:  DefaultHandshakeTimeout,
		EnableCompression: false,
	}

	conn, _, err := dialer.Dial(c.rpcURL, nil)
	if err != nil {
		return fmt.Errorf("failed to dial websocket: %w", err)
	}

	c.conn = conn
	return c.conn.SetReadDeadline(time.Now().Add(pongWait))
}

func (c *Client) receiveMessages() {
	reader := bufferPool.Get()
	defer bufferPool.Put(reader)

	for {
		select {
		case <-c.connCtx.Done():
			return
		default:
			messageType, reader, err := c.conn.NextReader()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					slog.Error("websocket unexpected close error", "err", err.Error())
				}
				c.closeAllSubscription(err)
				if c.reconnectOnErr {
					c.conn = nil
				}
				return
			}

			if messageType != websocket.TextMessage {
				continue
			}

			buf := rawMessagePool.Get().(*bytes.Buffer)
			buf.Reset()

			_, err = io.Copy(buf, reader)
			if err != nil {
				rawMessagePool.Put(buf)
				continue
			}

			select {
			case c.taskChan <- messageTask{buf: buf}:
			default:
				slog.Error("Worker pool is full, dropping message")
				rawMessagePool.Put(buf)
			}
		}
	}
}

func (c *Client) sendPing() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	if err := c.conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
		return fmt.Errorf("unable to send ping: %w", err)
	}

	return nil
}

func (c *Client) sendPong() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	if err := c.conn.WriteMessage(websocket.PongMessage, []byte{}); err != nil {
		return fmt.Errorf("unable to send pong: %w", err)
	}

	return nil
}

func (c *Client) Ping(timeout ...time.Duration) error {
	ch := c.pong.Done()

	if err := c.sendPing(); err != nil {
		return fmt.Errorf("unable to send ping: %w", err)
	}

	_timeout := pongWait
	if len(timeout) > 0 {
		_timeout = timeout[0]
	}

	select {
	case <-ch:
		return nil
	case <-time.After(_timeout):
		return fmt.Errorf("timeout waiting for pong")
	}
}

func (c *Client) Close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.connCtxCancel()
	if c.conn != nil {
		c.conn.Close()
	}
	close(c.taskChan)
}

func (c *Client) handleNewSubscriptionMessage(requestID uint64, parsed *resp) {
	if traceEnabled {
		slog.Debug("received new subscription message",
			slog.Uint64("message_id", requestID),
		)
	}

	sub, found := c.subscriptionByRequestID.Get(requestID)
	if !found {
		slog.Error("cannot find websocket message handler for a new stream",
			slog.Uint64("request_id", requestID),
		)
		return
	}

	if parsed.Error != nil {
		sub.err <- parsed.Error
		return
	}

	subID, ok := parsed.resultInt()
	if !ok {
		return
	}

	sub.subID = subID
	c.subscriptionByWSSubID.Set(subID, sub)

	slog.Debug("registered ws subscription",
		slog.Uint64("subscription_id", subID),
		slog.Uint64("request_id", requestID),
		slog.Int("subscription_count", c.subscriptionByWSSubID.Length()),
	)

	sub.err <- nil
}

func (c *Client) handleSubscriptionMessage(subID uint64, parsed *resp) {
	if traceEnabled {
		slog.Debug("received subscription message",
			slog.Uint64("subscription_id", subID),
		)
	}

	sub, found := c.subscriptionByWSSubID.Get(subID)
	if !found {
		// slog.Warn("unable to find subscription for ws message, unsubscribing",
		// 	slog.Uint64("subscription_id", subID),
		// 	"method", parsed.Method,
		// )
		//
		// method := parsed.Method
		// method = strings.TrimSuffix(method, "Notification")
		// method += "Unsubscribe"
		//
		// if err := c.unsubscribe(subID, method); err != nil {
		// 	slog.Warn("unable to send rpc unsubscribe call",
		// 		"err", err.Error(),
		// 		"subscription_id", subID,
		// 	)
		// }
		return
	}

	if parsed.Error != nil {
		sub.err <- parsed.Error
		return
	}

	result, err := sub.decoderFunc(parsed.Params.Result)
	if err != nil {
		c.closeSubscription(sub.req.ID, fmt.Errorf("unable to decode client response: %w", err))
		return
	}

	if len(sub.stream) >= cap(sub.stream) {
		slog.Warn("closing ws client subscription... not consuming fast enough",
			slog.Uint64("request_id", sub.req.ID),
		)
		c.closeSubscription(sub.req.ID, fmt.Errorf("reached channel max capacity %d", len(sub.stream)))
		return
	}

	if !sub.closed {
		sub.stream <- result
	}
}

func (c *Client) closeAllSubscription(err error) {
	for _, sub := range c.subscriptionByRequestID.GetAll() {
		sub.err <- err
	}

	c.subscriptionByRequestID = safemap.NewSafeMap[*Subscription]()
	c.subscriptionByWSSubID = safemap.NewSafeMap[*Subscription]()
}

func (c *Client) closeSubscription(reqID uint64, err error) {
	sub, found := c.subscriptionByRequestID.Get(reqID)
	if !found {
		return
	}

	sub.err <- err

	err = c.unsubscribe(sub.subID, sub.unsubscribeMethod)
	if err != nil {
		slog.Warn("unable to send rpc unsubscribe call",
			"err", err.Error(),
			"subscription_id", sub.subID,
			"request_id", reqID,
		)
	}

	c.subscriptionByRequestID.Delete(reqID)
	c.subscriptionByWSSubID.Delete(sub.subID)
}

func (c *Client) unsubscribe(subID uint64, method string) error {
	req := newRequest([]interface{}{subID}, method, nil, c.shortID)
	data, err := req.encode()
	if err != nil {
		return fmt.Errorf("unable to encode unsubscription message for subID %d and method %s", subID, method)
	}

	c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	err = c.conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		return fmt.Errorf("unable to send unsubscription message for subID %d and method %s", subID, method)
	}
	return nil
}

func (c *Client) subscribe(
	params []interface{},
	conf map[string]interface{},
	subscriptionMethod string,
	unsubscribeMethod string,
	decoderFunc decoderFunc,
) (*Subscription, error) {
	req := newRequest(params, subscriptionMethod, conf, c.shortID)
	data, err := req.encode()
	if err != nil {
		return nil, fmt.Errorf("subscribe: unable to encode subscription request: %w", err)
	}

	sub := newSubscription(
		req,
		func(err error) {
			c.closeSubscription(req.ID, err)
		},
		unsubscribeMethod,
		decoderFunc,
	)

	c.subscriptionByRequestID.Set(req.ID, sub)

	slog.Info("added new subscription to websocket client", slog.Int("count", c.subscriptionByRequestID.Length()))
	slog.Debug("writing data to conn",
		slog.Any("data", jjson.RawMessage(data)),
	)

	c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	if err = c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		c.subscriptionByRequestID.Delete(req.ID)
		return nil, fmt.Errorf("unable to write subscription request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if _, err := sub.Recv(ctx); err != nil {
		c.subscriptionByRequestID.Delete(req.ID)
		return nil, fmt.Errorf("unable to connect: %w", err)
	}

	return sub, nil
}

// Update processMessage function to use Sonic
func (c *Client) processMessage(buf *bytes.Buffer) {
	defer rawMessagePool.Put(buf)

	// Get message struct from pool
	parsed := messageTypePool.Get().(*resp)
	defer messageTypePool.Put(parsed)

	messageBytes := buf.Bytes()

	dec := sonicAPI.NewDecoder(bytes.NewReader(messageBytes))
	dec.UseNumber()
	if err := dec.Decode(parsed); err != nil {
		slog.Error("failed to decode message",
			"err", err.Error(),
			"msg_size", buf.Len(),
			"msg_content", buf.String()[:min(100, buf.Len())],
		)
		return
	}

	// Make a copy of the message for handlers
	if parsed.ID != "" {
		requestID, _ := strconv.ParseUint(parsed.ID.String(), 10, 64)
		c.handleNewSubscriptionMessage(requestID, parsed)
		return
	}

	if parsed.Params != nil {
		subID, _ := strconv.ParseUint(parsed.Params.Subscription.String(), 10, 64)
		c.handleSubscriptionMessage(subID, parsed)
		return
	}
}

// Update any other JSON handling functions
func decodeResponseFromReader(r io.Reader, reply interface{}) (err error) {
	return sonicAPI.NewDecoder(r).Decode(reply)
}

func decodeResponseFromMessage(r []byte, reply interface{}) (err error) {
	if len(r) == 0 {
		return fmt.Errorf("empty message in decodeResponseFromMessage")
	}

	return sonicAPI.Unmarshal(r, reply)
}
