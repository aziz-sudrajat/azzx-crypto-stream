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
	"sort"
	"sync"
	"time"
)

const (
	bybitWS   = "wss://stream.bybit.com/v5/public/linear"
	bybitREST = "https://api.bybit.com"
)

// Bybit is a public-data client for Bybit linear perpetuals.
type Bybit struct {
	mu    sync.Mutex
	books map[Symbol]*bybitBook
}

type bybitBook struct {
	bids map[float64]float64
	asks map[float64]float64
}

// NewBybit creates a Bybit client (keeps order-book state for deltas).
func NewBybit() *Bybit {
	return &Bybit{books: make(map[Symbol]*bybitBook)}
}

// Name implements the venue name.
func (b *Bybit) Name() string { return BybitName }

// StreamAll opens one multiplexed WS: tickers + orderbook.50 + liquidations
// for every symbol.
func (b *Bybit) StreamAll(ctx context.Context, syms []Symbol, sink Sink) {
	args := make([]string, 0, len(syms)*3)
	for _, s := range syms {
		m := Pair(BybitName, s)
		args = append(args, "tickers."+m, "orderbook.50."+m, "allLiquidation."+m)
	}
	RunWS(ctx, WSConfig{
		URL:       bybitWS,
		Subscribe: map[string]any{"op": "subscribe", "args": args},
		Ping:      map[string]any{"op": "ping"},
		PingEvery: 20 * time.Second,
		OnState:   func(st string) { sink.Status(BybitName, st) },
		Handle: func(msg []byte) error {
			topic := bybitTopic(msg)
			switch {
			case topic == "":
				return nil // pong / subscribe ack
			case startsWith(topic, "tickers."):
				if t, err := parseBybitTicker(msg); err == nil {
					sink.Ticker(t)
				}
			case startsWith(topic, "orderbook."):
				if ob, ok := b.applyBookMsg(msg); ok {
					sink.Book(ob)
				}
			case startsWith(topic, "allLiquidation."):
				for _, e := range parseBybitLiquidation(msg) {
					sink.Liq(e)
				}
			}
			return nil
		},
	})
}

// Funding returns the latest funding rate via REST.
func (b *Bybit) Funding(ctx context.Context, s Symbol) (FundingInfo, error) {
	var v struct {
		Result struct {
			List []struct {
				FundingRate string `json:"fundingRate"`
			} `json:"list"`
		} `json:"result"`
	}
	url := bybitREST + "/v5/market/tickers?category=linear&symbol=" + Pair(BybitName, s)
	if err := GetJSON(ctx, url, &v); err != nil {
		return FundingInfo{}, err
	}
	if len(v.Result.List) == 0 {
		return FundingInfo{}, fmt.Errorf("empty tickers response")
	}
	rate, err := ParseFloat(v.Result.List[0].FundingRate)
	if err != nil {
		return FundingInfo{}, fmt.Errorf("bad fundingRate: %w", err)
	}
	return FundingInfo{Exchange: BybitName, Sym: s, Rate: rate, Time: time.Now()}, nil
}

// OpenInterest returns open interest in USD notional via REST.
func (b *Bybit) OpenInterest(ctx context.Context, s Symbol) (OpenInterest, error) {
	var v struct {
		Result struct {
			List []struct {
				OpenInterest string `json:"openInterest"`
			} `json:"list"`
		} `json:"result"`
	}
	url := bybitREST + "/v5/market/open-interest?category=linear&symbol=" + Pair(BybitName, s) + "&intervalTime=5min&limit=1"
	if err := GetJSON(ctx, url, &v); err != nil {
		return OpenInterest{}, err
	}
	if len(v.Result.List) == 0 {
		return OpenInterest{}, fmt.Errorf("empty open-interest response")
	}
	val, err := ParseFloat(v.Result.List[0].OpenInterest)
	if err != nil {
		return OpenInterest{}, fmt.Errorf("bad openInterest: %w", err)
	}
	return OpenInterest{Exchange: BybitName, Sym: s, Value: val, Time: time.Now()}, nil
}

// LongShortRatio returns the account long/short ratio via REST.
func (b *Bybit) LongShortRatio(ctx context.Context, s Symbol, period string) (LSRatio, error) {
	var v struct {
		Result struct {
			List []struct {
				BuyRatio  string `json:"buyRatio"`
				SellRatio string `json:"sellRatio"`
			} `json:"list"`
		} `json:"result"`
	}
	url := fmt.Sprintf("%s/v5/market/account-ratio?category=linear&symbol=%s&period=%s&limit=1",
		bybitREST, Pair(BybitName, s), period)
	if err := GetJSON(ctx, url, &v); err != nil {
		return LSRatio{}, err
	}
	if len(v.Result.List) == 0 {
		return LSRatio{}, fmt.Errorf("empty account-ratio response")
	}
	buy, err := ParseFloat(v.Result.List[0].BuyRatio)
	if err != nil {
		return LSRatio{}, fmt.Errorf("bad buyRatio: %w", err)
	}
	longPct := buy * 100
	return LSRatio{
		Exchange: BybitName, Sym: s, Period: period,
		LongPct: longPct, ShortPct: 100 - longPct, Time: time.Now(),
	}, nil
}

// --- pure parsers (unit-tested) ---

func bybitTopic(msg []byte) string {
	var v struct {
		Topic string `json:"topic"`
	}
	if err := json.Unmarshal(msg, &v); err != nil {
		return ""
	}
	return v.Topic
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func parseBybitTicker(msg []byte) (Ticker, error) {
	var v struct {
		Topic string `json:"topic"`
		Data  struct {
			LastPrice string `json:"lastPrice"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg, &v); err != nil {
		return Ticker{}, err
	}
	market := v.Topic[len("tickers."):]
	sym, ok := Unpair(BybitName, market)
	if !ok {
		return Ticker{}, fmt.Errorf("unsupported market %s", market)
	}
	price, err := ParseFloat(v.Data.LastPrice)
	if err != nil {
		return Ticker{}, err
	}
	return Ticker{Exchange: BybitName, Sym: sym, Price: price, Time: time.Now()}, nil
}

// applyBookMsg applies a Bybit orderbook snapshot or delta and returns the
// rebuilt top book. ok=false when the message could not be applied.
func (b *Bybit) applyBookMsg(msg []byte) (ob OrderBook, ok bool) {
	var v struct {
		Topic string `json:"topic"`
		Type  string `json:"type"`
		Data  struct {
			S string     `json:"s"`
			B [][]string `json:"b"`
			A [][]string `json:"a"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg, &v); err != nil {
		return OrderBook{}, false
	}
	market := v.Data.S
	if market == "" && len(v.Topic) > len("orderbook.50.") {
		market = v.Topic[len("orderbook.50."):]
	}
	sym, good := Unpair(BybitName, market)
	if !good {
		return OrderBook{}, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	st, exists := b.books[sym]
	if !exists || v.Type == "snapshot" {
		st = &bybitBook{bids: map[float64]float64{}, asks: map[float64]float64{}}
		b.books[sym] = st
	}
	applyLevels := func(dst map[float64]float64, raw [][]string) {
		for _, r := range raw {
			if len(r) < 2 {
				continue
			}
			p, err1 := ParseFloat(r[0])
			q, err2 := ParseFloat(r[1])
			if err1 != nil || err2 != nil {
				continue
			}
			if q == 0 {
				delete(dst, p)
			} else {
				dst[p] = q
			}
		}
	}
	applyLevels(st.bids, v.Data.B)
	applyLevels(st.asks, v.Data.A)
	return OrderBook{
		Exchange: BybitName, Sym: sym,
		Bids: topLevels(st.bids, false, 20),
		Asks: topLevels(st.asks, true, 20),
		Time: time.Now(),
	}, true
}

func topLevels(m map[float64]float64, ascending bool, n int) []DepthLevel {
	out := make([]DepthLevel, 0, len(m))
	for p, q := range m {
		out = append(out, DepthLevel{Price: p, Qty: q})
	}
	sort.Slice(out, func(i, j int) bool {
		if ascending {
			return out[i].Price < out[j].Price
		}
		return out[i].Price > out[j].Price
	})
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// rawStr extracts a string field with an exact, case-sensitive key lookup.
// Bybit sends both "s" (symbol) and "S" (side); encoding/json fold-matches
// "S" onto a field tagged `json:"s"`, so struct unmarshalling is unsafe here.
func rawStr(m map[string]json.RawMessage, key string) string {
	var s string
	if err := json.Unmarshal(m[key], &s); err == nil {
		return s
	}
	return ""
}

func rawInt(m map[string]json.RawMessage, key string) int64 {
	var n int64
	if err := json.Unmarshal(m[key], &n); err == nil {
		return n
	}
	return 0
}

func parseBybitLiquidation(msg []byte) []LiqEvent {
	var v struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(msg, &v); err != nil {
		return nil
	}
	var out []LiqEvent
	for _, d := range v.Data {
		sym, ok := Unpair(BybitName, rawStr(d, "s"))
		if !ok {
			continue
		}
		qty, err1 := ParseFloat(rawStr(d, "v"))
		price, err2 := ParseFloat(rawStr(d, "p"))
		if err1 != nil || err2 != nil {
			continue
		}
		side := "SHORT"
		if rawStr(d, "S") == "Sell" {
			side = "LONG" // a Sell force order closes a long position
		}
		out = append(out, LiqEvent{
			Exchange: BybitName, Sym: sym, Side: side,
			Qty: qty, Price: price, USD: qty * price,
			Time: time.UnixMilli(rawInt(d, "T")),
		})
	}
	return out
}
