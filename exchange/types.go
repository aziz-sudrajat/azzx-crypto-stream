// Copyright (c) 2026 aziz-sudrajat <azizsudrajat.stb4@gmail.com>
// https://github.com/aziz-sudrajat
//
// All rights reserved. This file is part of the azzx-crypto-stream project.
// Unauthorized copying, modification, distribution, or false claim of
// authorship is prohibited.

// Package exchange defines the venue-neutral data types shared by every
// exchange client and the TUI.
package exchange

import "time"

// Symbol is a base asset traded as a USDT perpetual.
type Symbol string

const (
	BTC Symbol = "BTC"
	ETH Symbol = "ETH"
	SOL Symbol = "SOL"
)

// Symbols is the supported universe, in display order.
var Symbols = []Symbol{BTC, ETH, SOL}

// Exchange names used as map keys and display labels.
const (
	BinanceName = "Binance"
	BybitName   = "Bybit"
	OKXName     = "OKX"
)

// Exchanges lists the venues in display order.
var Exchanges = []string{BinanceName, BybitName, OKXName}

// Ticker is a live price tick from one venue.
type Ticker struct {
	Exchange string
	Sym      Symbol
	Price    float64
	Time     time.Time
}

// FundingInfo is the latest funding rate of a perpetual.
type FundingInfo struct {
	Exchange string
	Sym      Symbol
	Rate     float64 // e.g. 0.0001 means 0.01%
	Time     time.Time
}

// OpenInterest is the open interest of a perpetual in USD notional.
type OpenInterest struct {
	Exchange string
	Sym      Symbol
	Value    float64 // USD
	Time     time.Time
}

// DepthLevel is one price level of an order book.
type DepthLevel struct {
	Price float64
	Qty   float64 // base asset units
}

// OrderBook is a top-of-book snapshot.
type OrderBook struct {
	Exchange string
	Sym      Symbol
	Bids     []DepthLevel // descending by price
	Asks     []DepthLevel // ascending by price
	Time     time.Time
}

// LiqEvent is a single liquidation fill.
type LiqEvent struct {
	Exchange string
	Sym      Symbol
	Side     string // "LONG" or "SHORT": the side that got liquidated
	Qty      float64
	Price    float64
	USD      float64
	Time     time.Time
}

// LSRatio is the long/short account ratio of a venue.
type LSRatio struct {
	Exchange string
	Sym      Symbol
	Period   string // "4h" or "1d"
	LongPct  float64
	ShortPct float64
	Time     time.Time
}

// Pair returns the venue-specific market id for a symbol.
func Pair(exchange string, s Symbol) string {
	if exchange == OKXName {
		return string(s) + "-USDT-SWAP"
	}
	return string(s) + "USDT"
}

// Unpair converts a venue market id back to a Symbol. ok=false when the
// market is not one of the supported symbols.
func Unpair(exchange, market string) (s Symbol, ok bool) {
	for _, sym := range Symbols {
		if Pair(exchange, sym) == market {
			return sym, true
		}
	}
	return "", false
}

// LSPeriods are the selectable long/short ratio timeframes.
var LSPeriods = []string{"4h", "1d"}

// Sink receives live data from exchange clients. Every method must be safe
// for concurrent use; implementations typically forward into the UI.
type Sink interface {
	Ticker(Ticker)
	Book(OrderBook)
	Liq(LiqEvent)
	Status(exchange string, state string)
}
