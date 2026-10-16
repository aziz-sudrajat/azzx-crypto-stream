// Copyright (c) 2026 aziz-sudrajat <azizsudrajat.stb4@gmail.com>
// https://github.com/aziz-sudrajat
//
// All rights reserved. This file is part of the azzx-crypto-stream project.
// Unauthorized copying, modification, distribution, or false claim of
// authorship is prohibited.

package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

var httpClient = &http.Client{Timeout: 12 * time.Second}

// GetJSON performs a GET request and decodes the JSON body into v.
// Non-2xx responses are returned as errors (fail-soft: the caller decides).
func GetJSON(ctx context.Context, url string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "azzx-crypto-stream/1.0")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

// ParseFloat parses the string-encoded numbers crypto APIs love to send.
func ParseFloat(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

// Backoff returns how long to wait before reconnect attempt n (0-based),
// capped at 30s: 1s, 2s, 4s, 8s, 16s, 30s, 30s, ...
func Backoff(n int) time.Duration {
	if n < 0 {
		n = 0
	}
	d := time.Duration(1<<uint(n)) * time.Second
	if d > 30*time.Second || n > 5 {
		d = 30 * time.Second
	}
	return d
}

// WSConfig describes one resilient websocket subscription.
type WSConfig struct {
	URL       string
	Subscribe any                    // marshalled to JSON right after connect; nil = no subscribe frame
	Ping      any                    // marshalled to JSON every PingEvery; nil = no client ping
	PingEvery time.Duration          // default 20s when Ping != nil
	Handle    func(msg []byte) error // called for every text message
	OnState   func(state string)     // "connected" | "error: ..." | "reconnecting"
}

// RunWS keeps a websocket subscription alive until ctx is cancelled,
// reconnecting with backoff on any failure.
func RunWS(ctx context.Context, cfg WSConfig) {
	if cfg.Ping != nil && cfg.PingEvery == 0 {
		cfg.PingEvery = 20 * time.Second
	}
	report := func(s string) {
		if cfg.OnState != nil {
			cfg.OnState(s)
		}
	}
	attempt := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, cfg.URL, nil)
		if err != nil {
			report("error: " + shortErr(err))
			time.Sleep(Backoff(attempt))
			attempt++
			continue
		}
		if cfg.Subscribe != nil {
			if err := conn.WriteJSON(cfg.Subscribe); err != nil {
				conn.Close()
				report("error: " + shortErr(err))
				time.Sleep(Backoff(attempt))
				attempt++
				continue
			}
		}
		attempt = 0
		report("connected")
		if !pumpMessages(ctx, conn, cfg) {
			return // ctx cancelled
		}
		report("reconnecting")
	}
}

// pumpMessages reads until the connection dies or ctx is cancelled.
// Returns false when ctx was cancelled (caller should stop entirely).
func pumpMessages(ctx context.Context, conn *websocket.Conn, cfg WSConfig) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			_ = cfg.Handle(msg) // never let a bad message kill the stream
		}
	}()

	var pingTick <-chan time.Time
	if cfg.Ping != nil {
		t := time.NewTicker(cfg.PingEvery)
		defer t.Stop()
		pingTick = t.C
	}
	for {
		select {
		case <-ctx.Done():
			conn.Close()
			<-done
			return false
		case <-done:
			conn.Close()
			return true
		case <-pingTick:
			_ = conn.WriteJSON(cfg.Ping)
		}
	}
}

func shortErr(err error) string {
	s := err.Error()
	if len(s) > 80 {
		s = s[:80] + "…"
	}
	return s
}
