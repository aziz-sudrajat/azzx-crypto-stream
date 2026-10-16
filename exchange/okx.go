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
	"strings"
	"sync"
	"time"
)

const (
	okxWS   = "wss://ws.okx.com:8443/ws/v5/public"
	okxREST = "https://www.okx.com"
)

// OKX is a public-data client for OKX USDT perpetual swaps.
type OKX struct {
	mu    sync.Mutex
	books map[Symbol]*okxBook
}

type okxBook struct {
	bids map[float64]float64
	asks map[float64]float64
}

// NewOKX creates an OKX client (keeps order-book state for updates).
func NewOKX() *OKX {
	return &OKX{books: make(map[Symbol]*okxBook)}
}

// Name implements the venue name.
func (o *OKX) Name() string { return OKXName }

// StreamAll opens one multiplexed WS: tickers + books per symbol and the
// account-wide liquidation-orders channel.
func (o *OKX) StreamAll(ctx context.Context, syms []Symbol, sink Sink) {
	args := make([]map[string]string, 0, len(syms)*2+1)
	for _, s := range syms {
		inst := Pair(OKXName, s)
		args = append(args,
			map[string]string{"channel": "tickers", "instId": inst},
			map[string]string{"channel": "books", "instId": inst},
		)
	}
	args = append(args, map[string]string{"channel": "liquidation-orders", "instType": "SWAP"})
	RunWS(ctx, WSConfig{
		URL:       okxWS,
		Subscribe: map[string]any{"op": "subscribe", "args": args},
		Ping:      "ping",
		PingEvery: 20 * time.Second,
		OnState:   func(st string) { sink.Status(OKXName, st) },
		Handle: func(msg []byte) error {
			if string(msg) == "pong" {
				return nil
			}
			ch, inst := okxChannel(msg)
			switch ch {
			case "tickers":
				if t, err := parseOKXTicker(inst, msg); err == nil {
					sink.Ticker(t)
				}
			case "books":
				if ob, ok := o.applyBookMsg(inst, msg); ok {
					sink.Book(ob)
				}
			case "liquidation-orders":
				for _, e := range parseOKXLiquidations(msg) {
					sink.Liq(e)
				}
			}
			return nil
		},
	})
}

// Funding returns the latest funding rate via REST.
func (o *OKX) Funding(ctx context.Context, s Symbol) (FundingInfo, error) {
	var v struct {
		Data []struct {
			FundingRate string `json:"fundingRate"`
		} `json:"data"`
	}
	url := okxREST + "/api/v5/public/funding-rate?instId=" + Pair(OKXName, s)
	if err := GetJSON(ctx, url, &v); err != nil {
		return FundingInfo{}, err
	}
	if len(v.Data) == 0 {
		return FundingInfo{}, fmt.Errorf("empty funding-rate response")
	}
	rate, err := ParseFloat(v.Data[0].FundingRate)
	if err != nil {
		return FundingInfo{}, fmt.Errorf("bad fundingRate: %w", err)
	}
	return FundingInfo{Exchange: OKXName, Sym: s, Rate: rate, Time: time.Now()}, nil
}

// OpenInterest returns open interest in USD notional via REST.
func (o *OKX) OpenInterest(ctx context.Context, s Symbol) (OpenInterest, error) {
	var v struct {
		Data []struct {
			OiUsd string `json:"oiUsd"`
		} `json:"data"`
	}
	url := okxREST + "/api/v5/public/open-interest?instType=SWAP&instId=" + Pair(OKXName, s)
	if err := GetJSON(ctx, url, &v); err != nil {
		return OpenInterest{}, err
	}
	if len(v.Data) == 0 {
		return OpenInterest{}, fmt.Errorf("empty open-interest response")
	}
	val, err := ParseFloat(v.Data[0].OiUsd)
	if err != nil {
		return OpenInterest{}, fmt.Errorf("bad oiUsd: %w", err)
	}
	return OpenInterest{Exchange: OKXName, Sym: s, Value: val, Time: time.Now()}, nil
}

// LongShortRatio returns the long/short account ratio via REST.
func (o *OKX) LongShortRatio(ctx context.Context, s Symbol, period string) (LSRatio, error) {
	okxPeriod := strings.ToUpper(period) // "4h" -> "4H", "1d" -> "1D"
	var v struct {
		Data []struct {
			Ratio string `json:"ratio"`
		} `json:"data"`
	}
	url := fmt.Sprintf("%s/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy=%s&period=%s",
		okxREST, string(s), okxPeriod)
	if err := GetJSON(ctx, url, &v); err != nil {
		return LSRatio{}, err
	}
	if len(v.Data) == 0 {
		return LSRatio{}, fmt.Errorf("empty long-short-account-ratio response")
	}
	ratio, err := ParseFloat(v.Data[0].Ratio)
	if err != nil {
		return LSRatio{}, fmt.Errorf("bad ratio: %w", err)
	}
	longPct := ratio / (1 + ratio) * 100
	return LSRatio{
		Exchange: OKXName, Sym: s, Period: period,
		LongPct: longPct, ShortPct: 100 - longPct, Time: time.Now(),
	}, nil
}

// --- pure parsers (unit-tested) ---

func okxChannel(msg []byte) (channel, instId string) {
	var v struct {
		Arg struct {
			Channel string `json:"channel"`
			InstID  string `json:"instId"`
		} `json:"arg"`
	}
	if err := json.Unmarshal(msg, &v); err != nil {
		return "", ""
	}
	return v.Arg.Channel, v.Arg.InstID
}

func parseOKXTicker(instId string, msg []byte) (Ticker, error) {
	var v struct {
		Data []struct {
			Last string `json:"last"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg, &v); err != nil {
		return Ticker{}, err
	}
	if len(v.Data) == 0 {
		return Ticker{}, fmt.Errorf("empty ticker data")
	}
	sym, ok := Unpair(OKXName, instId)
	if !ok {
		return Ticker{}, fmt.Errorf("unsupported instId %s", instId)
	}
	price, err := ParseFloat(v.Data[0].Last)
	if err != nil {
		return Ticker{}, err
	}
	return Ticker{Exchange: OKXName, Sym: sym, Price: price, Time: time.Now()}, nil
}

// applyBookMsg applies an OKX books snapshot or update.
func (o *OKX) applyBookMsg(instId string, msg []byte) (ob OrderBook, ok bool) {
	var v struct {
		Action string `json:"action"`
		Data   []struct {
			Asks [][]string `json:"asks"`
			Bids [][]string `json:"bids"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg, &v); err != nil || len(v.Data) == 0 {
		return OrderBook{}, false
	}
	sym, good := Unpair(OKXName, instId)
	if !good {
		return OrderBook{}, false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	st, exists := o.books[sym]
	if !exists || v.Action == "snapshot" {
		st = &okxBook{bids: map[float64]float64{}, asks: map[float64]float64{}}
		o.books[sym] = st
	}
	apply := func(dst map[float64]float64, raw [][]string) {
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
	apply(st.bids, v.Data[0].Bids)
	apply(st.asks, v.Data[0].Asks)
	return OrderBook{
		Exchange: OKXName, Sym: sym,
		Bids: topLevels(st.bids, false, 20),
		Asks: topLevels(st.asks, true, 20),
		Time: time.Now(),
	}, true
}

func parseOKXLiquidations(msg []byte) []LiqEvent {
	var v struct {
		Data []struct {
			InstID  string `json:"instId"`
			Px      string `json:"px"`
			Sz      string `json:"sz"`
			PosSide string `json:"posSide"` // "long" | "short": side liquidated
			CTime   string `json:"cTime"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg, &v); err != nil {
		return nil
	}
	var out []LiqEvent
	for _, d := range v.Data {
		sym, ok := Unpair(OKXName, d.InstID)
		if !ok {
			continue
		}
		price, err1 := ParseFloat(d.Px)
		qty, err2 := ParseFloat(d.Sz)
		if err1 != nil || err2 != nil {
			continue
		}
		side := "SHORT"
		if d.PosSide == "long" {
			side = "LONG"
		}
		var ts time.Time
		if ms, err := ParseFloat(d.CTime); err == nil {
			ts = time.UnixMilli(int64(ms))
		} else {
			ts = time.Now()
		}
		out = append(out, LiqEvent{
			Exchange: OKXName, Sym: sym, Side: side,
			Qty: qty, Price: price, USD: qty * price, Time: ts,
		})
	}
	return out
}
