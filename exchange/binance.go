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
	"strings"
	"time"
)

const (
	binanceWS   = "wss://fstream.binance.com/ws"
	binanceREST = "https://fapi.binance.com"
)

// Binance is a public-data client for Binance USD-M futures.
type Binance struct{}

// Name implements the venue name.
func (Binance) Name() string { return BinanceName }

// StreamTickers streams 24h mini-tickers for every symbol.
func (b Binance) StreamTickers(ctx context.Context, syms []Symbol, sink Sink) {
	params := make([]string, 0, len(syms))
	for _, s := range syms {
		params = append(params, strings.ToLower(Pair(BinanceName, s))+"@miniTicker")
	}
	RunWS(ctx, WSConfig{
		URL:       binanceWS,
		Subscribe: map[string]any{"method": "SUBSCRIBE", "params": params, "id": 1},
		OnState:   func(st string) { sink.Status(BinanceName, st) },
		Handle: func(msg []byte) error {
			t, err := parseBinanceMiniTicker(msg)
			if err != nil {
				return nil // ignore non-ticker frames (e.g. subscribe acks)
			}
			sink.Ticker(t)
			return nil
		},
	})
}

// StreamDepth streams depth20 snapshots for every symbol.
func (b Binance) StreamDepth(ctx context.Context, syms []Symbol, sink Sink) {
	params := make([]string, 0, len(syms))
	for _, s := range syms {
		params = append(params, strings.ToLower(Pair(BinanceName, s))+"@depth20@100ms")
	}
	RunWS(ctx, WSConfig{
		URL:       binanceWS,
		Subscribe: map[string]any{"method": "SUBSCRIBE", "params": params, "id": 2},
		OnState:   func(st string) { sink.Status(BinanceName, st) },
		Handle: func(msg []byte) error {
			ob, err := parseBinanceDepth(msg)
			if err != nil {
				return nil
			}
			sink.Book(ob)
			return nil
		},
	})
}

// StreamLiquidations streams the all-market forceOrder feed.
func (b Binance) StreamLiquidations(ctx context.Context, sink Sink) {
	RunWS(ctx, WSConfig{
		URL:       binanceWS,
		Subscribe: map[string]any{"method": "SUBSCRIBE", "params": []string{"!forceOrder@arr"}, "id": 3},
		OnState:   func(st string) { sink.Status(BinanceName, st) },
		Handle: func(msg []byte) error {
			for _, e := range parseBinanceForceOrders(msg) {
				sink.Liq(e)
			}
			return nil
		},
	})
}

// Funding returns the latest funding rate.
func (b Binance) Funding(ctx context.Context, s Symbol) (FundingInfo, error) {
	var v struct {
		LastFundingRate string `json:"lastFundingRate"`
	}
	url := binanceREST + "/fapi/v1/premiumIndex?symbol=" + Pair(BinanceName, s)
	if err := GetJSON(ctx, url, &v); err != nil {
		return FundingInfo{}, err
	}
	rate, err := ParseFloat(v.LastFundingRate)
	if err != nil {
		return FundingInfo{}, fmt.Errorf("bad fundingRate: %w", err)
	}
	return FundingInfo{Exchange: BinanceName, Sym: s, Rate: rate, Time: time.Now()}, nil
}

// OpenInterest returns open interest in USD notional.
func (b Binance) OpenInterest(ctx context.Context, s Symbol) (OpenInterest, error) {
	var oi struct {
		OpenInterest string `json:"openInterest"` // base-asset units
	}
	if err := GetJSON(ctx, binanceREST+"/fapi/v1/openInterest?symbol="+Pair(BinanceName, s), &oi); err != nil {
		return OpenInterest{}, err
	}
	base, err := ParseFloat(oi.OpenInterest)
	if err != nil {
		return OpenInterest{}, fmt.Errorf("bad openInterest: %w", err)
	}
	var px struct {
		MarkPrice string `json:"markPrice"`
	}
	if err := GetJSON(ctx, binanceREST+"/fapi/v1/premiumIndex?symbol="+Pair(BinanceName, s), &px); err != nil {
		return OpenInterest{}, err
	}
	mark, err := ParseFloat(px.MarkPrice)
	if err != nil {
		return OpenInterest{}, fmt.Errorf("bad markPrice: %w", err)
	}
	return OpenInterest{Exchange: BinanceName, Sym: s, Value: base * mark, Time: time.Now()}, nil
}

// LongShortRatio returns the global long/short account ratio.
func (b Binance) LongShortRatio(ctx context.Context, s Symbol, period string) (LSRatio, error) {
	var arr []struct {
		LongShortRatio string `json:"longShortRatio"`
	}
	url := fmt.Sprintf("%s/futures/data/globalLongShortAccountRatio?symbol=%s&period=%s&limit=1",
		binanceREST, Pair(BinanceName, s), period)
	if err := GetJSON(ctx, url, &arr); err != nil {
		return LSRatio{}, err
	}
	if len(arr) == 0 {
		return LSRatio{}, fmt.Errorf("empty longShortRatio response")
	}
	ratio, err := ParseFloat(arr[0].LongShortRatio)
	if err != nil {
		return LSRatio{}, fmt.Errorf("bad longShortRatio: %w", err)
	}
	longPct := ratio / (1 + ratio) * 100
	return LSRatio{
		Exchange: BinanceName, Sym: s, Period: period,
		LongPct: longPct, ShortPct: 100 - longPct, Time: time.Now(),
	}, nil
}

// binanceEventName extracts the "e" event name with an exact,
// case-sensitive key lookup. encoding/json fold-matches the numeric "E"
// timestamp key onto a field tagged `json:"e"`, which breaks unmarshalling,
// so structs must not declare such a field.
func binanceEventName(msg []byte) string {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(msg, &m); err != nil {
		return ""
	}
	var name string
	if err := json.Unmarshal(m["e"], &name); err != nil {
		return ""
	}
	return name
}

// --- pure parsers (unit-tested) ---

func parseBinanceMiniTicker(msg []byte) (Ticker, error) {
	if binanceEventName(msg) != "24hrMiniTicker" {
		return Ticker{}, fmt.Errorf("not a miniTicker")
	}
	var v struct {
		S string `json:"s"`
		C string `json:"c"`
	}
	if err := json.Unmarshal(msg, &v); err != nil {
		return Ticker{}, err
	}
	sym, ok := Unpair(BinanceName, v.S)
	if !ok {
		return Ticker{}, fmt.Errorf("unsupported symbol %s", v.S)
	}
	price, err := ParseFloat(v.C)
	if err != nil {
		return Ticker{}, err
	}
	return Ticker{Exchange: BinanceName, Sym: sym, Price: price, Time: time.Now()}, nil
}

func parseBinanceDepth(msg []byte) (OrderBook, error) {
	if binanceEventName(msg) != "depthUpdate" {
		return OrderBook{}, fmt.Errorf("not a depthUpdate")
	}
	var v struct {
		S    string     `json:"s"`
		Bids [][]string `json:"b"`
		Asks [][]string `json:"a"`
	}
	if err := json.Unmarshal(msg, &v); err != nil {
		return OrderBook{}, err
	}
	sym, ok := Unpair(BinanceName, v.S)
	if !ok {
		return OrderBook{}, fmt.Errorf("unsupported symbol %s", v.S)
	}
	toLevels := func(raw [][]string) ([]DepthLevel, error) {
		out := make([]DepthLevel, 0, len(raw))
		for _, r := range raw {
			if len(r) < 2 {
				continue
			}
			p, err1 := ParseFloat(r[0])
			q, err2 := ParseFloat(r[1])
			if err1 != nil || err2 != nil {
				continue
			}
			out = append(out, DepthLevel{Price: p, Qty: q})
		}
		return out, nil
	}
	bids, _ := toLevels(v.Bids)
	asks, _ := toLevels(v.Asks)
	sort.Slice(bids, func(i, j int) bool { return bids[i].Price > bids[j].Price })
	sort.Slice(asks, func(i, j int) bool { return asks[i].Price < asks[j].Price })
	return OrderBook{Exchange: BinanceName, Sym: sym, Bids: bids, Asks: asks, Time: time.Now()}, nil
}

// parseBinanceForceOrders parses a !forceOrder@arr frame, which may carry
// several liquidation orders at once.
func parseBinanceForceOrders(msg []byte) []LiqEvent {
	if binanceEventName(msg) != "forceOrder" {
		return nil
	}
	var v struct {
		O []struct {
			S string `json:"s"` // market, e.g. BTCUSDT
			O string `json:"S"` // order side: SELL liquidates a LONG
			Q string `json:"q"`
			P string `json:"p"` // fill price
			T int64  `json:"T"`
		} `json:"o"`
	}
	if err := json.Unmarshal(msg, &v); err != nil {
		return nil
	}
	var out []LiqEvent
	for _, o := range v.O {
		sym, ok := Unpair(BinanceName, o.S)
		if !ok {
			continue
		}
		qty, err1 := ParseFloat(o.Q)
		price, err2 := ParseFloat(o.P)
		if err1 != nil || err2 != nil {
			continue
		}
		side := "SHORT"
		if o.O == "SELL" {
			side = "LONG" // a SELL force order closes a long position
		}
		out = append(out, LiqEvent{
			Exchange: BinanceName, Sym: sym, Side: side,
			Qty: qty, Price: price, USD: qty * price,
			Time: time.UnixMilli(o.T),
		})
	}
	return out
}
