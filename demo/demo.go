// Copyright (c) 2026 aziz-sudrajat <azizsudrajat.stb4@gmail.com>
// https://github.com/aziz-sudrajat
//
// All rights reserved. This file is part of the azzx-crypto-stream project.
// Unauthorized copying, modification, distribution, or false claim of
// authorship is prohibited.

// Package demo generates realistic simulated market data for the --demo
// flag. Demo mode exists ONLY for recording the README demo GIF; simulated
// data is never used for trading decisions. Normal mode uses 100% live data.
package demo

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/aziz-sudrajat/azzx-crypto-stream/exchange"
)

// Feed receives simulated data. It mirrors the live data path so the TUI
// renders identically in demo and live modes.
type Feed interface {
	Ticker(exchange.Ticker)
	Book(exchange.OrderBook)
	Liq(exchange.LiqEvent)
	Funding(exchange.FundingInfo)
	OI(exchange.OpenInterest)
	LS(exchange.LSRatio)
	Status(exchange string, state string)
}

var (
	basePrice = map[exchange.Symbol]float64{
		exchange.BTC: 97412.5,
		exchange.ETH: 3412.44,
		exchange.SOL: 214.322,
	}
	baseQty = map[exchange.Symbol]float64{ // typical liq qty scale
		exchange.BTC: 0.4,
		exchange.ETH: 4.0,
		exchange.SOL: 60.0,
	}
	bookQty = map[exchange.Symbol]float64{ // typical book level qty
		exchange.BTC: 2.0,
		exchange.ETH: 25.0,
		exchange.SOL: 150.0,
	}
	baseFunding = map[exchange.Symbol]float64{
		exchange.BTC: 0.000100,
		exchange.ETH: 0.000085,
		exchange.SOL: -0.000021,
	}
	baseOI = map[exchange.Symbol]map[string]float64{ // USD notional
		exchange.BTC: {exchange.BinanceName: 8.5e9, exchange.BybitName: 4.2e9, exchange.OKXName: 3.1e9},
		exchange.ETH: {exchange.BinanceName: 2.1e9, exchange.BybitName: 1.1e9, exchange.OKXName: 0.9e9},
		exchange.SOL: {exchange.BinanceName: 0.8e9, exchange.BybitName: 0.42e9, exchange.OKXName: 0.35e9},
	}
	baseLS = map[string]float64{ // long% per exchange
		exchange.BinanceName: 49.6,
		exchange.BybitName:   48.5,
		exchange.OKXName:     50.3,
	}
	// exOffset gives each venue a slightly different price, like reality.
	exOffset = map[string]float64{
		exchange.BinanceName: 0,
		exchange.BybitName:   -0.0002,
		exchange.OKXName:     0.0003,
	}
)

// sim holds the evolving simulated market state.
type sim struct {
	feed  Feed
	rng   *rand.Rand
	mu    sync.Mutex
	price map[exchange.Symbol]float64
	fund  map[exchange.Symbol]map[string]float64
	oi    map[exchange.Symbol]map[string]float64
}

// Run streams simulated data until ctx is cancelled.
// demo mode — simulated data for README recording only, never used for trading decisions.
func Run(ctx context.Context, feed Feed) {
	s := &sim{
		feed:  feed,
		rng:   rand.New(rand.NewSource(42)), // deterministic: same recording every run
		price: map[exchange.Symbol]float64{},
		fund:  map[exchange.Symbol]map[string]float64{},
		oi:    map[exchange.Symbol]map[string]float64{},
	}
	for _, sym := range exchange.Symbols {
		s.price[sym] = basePrice[sym]
		s.fund[sym] = map[string]float64{}
		s.oi[sym] = map[string]float64{}
		for _, e := range exchange.Exchanges {
			s.fund[sym][e] = baseFunding[sym]
			s.oi[sym][e] = baseOI[sym][e]
		}
	}
	for _, e := range exchange.Exchanges {
		feed.Status(e, "connected")
	}
	s.pushFunding()
	s.pushOI()
	s.pushLS()

	go s.loop(ctx, 250*time.Millisecond, s.pushTickers)
	go s.loop(ctx, 500*time.Millisecond, s.pushBooks)
	go s.loop(ctx, 5*time.Second, func() { s.pushFunding(); s.pushOI() })
	go s.loop(ctx, 10*time.Second, s.pushLS)
	go s.liqLoop(ctx)
	<-ctx.Done()
}

func (s *sim) loop(ctx context.Context, d time.Duration, fn func()) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn()
		}
	}
}

func (s *sim) priceOf(sym exchange.Symbol) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.price[sym]
}

func (s *sim) pushTickers() {
	for _, sym := range exchange.Symbols {
		// random walk ±0.04%
		s.mu.Lock()
		s.price[sym] *= 1 + (s.rng.Float64()-0.5)*0.0008
		px := s.price[sym]
		s.mu.Unlock()
		for _, e := range exchange.Exchanges {
			s.feed.Ticker(exchange.Ticker{
				Exchange: e, Sym: sym,
				Price: px * (1 + exOffset[e]),
				Time:  time.Now(),
			})
		}
	}
}

func (s *sim) pushBooks() {
	for _, sym := range exchange.Symbols {
		mid := s.priceOf(sym)
		for _, e := range exchange.Exchanges {
			m := mid * (1 + exOffset[e])
			s.feed.Book(makeBook(e, sym, m, s.rng))
		}
	}
}

func makeBook(ex string, sym exchange.Symbol, mid float64, rng *rand.Rand) exchange.OrderBook {
	const n = 20
	spread := mid * 0.0002
	bids := make([]exchange.DepthLevel, 0, n)
	asks := make([]exchange.DepthLevel, 0, n)
	q0 := bookQty[sym]
	bidPx := mid - spread/2
	askPx := mid + spread/2
	for i := 0; i < n; i++ {
		// cumulative gaps keep levels strictly ordered
		bidPx -= spread * (0.15 + rng.Float64()*0.5)
		askPx += spread * (0.15 + rng.Float64()*0.5)
		qty := q0 * (0.4 + rng.Float64()*2.2)
		if rng.Float64() < 0.06 { // occasional wall
			qty *= 4 + rng.Float64()*4
		}
		bids = append(bids, exchange.DepthLevel{Price: bidPx, Qty: qty})
		aqty := q0 * (0.4 + rng.Float64()*2.2)
		if rng.Float64() < 0.06 {
			aqty *= 4 + rng.Float64()*4
		}
		asks = append(asks, exchange.DepthLevel{Price: askPx, Qty: aqty})
	}
	return exchange.OrderBook{Exchange: ex, Sym: sym, Bids: bids, Asks: asks, Time: time.Now()}
}

func (s *sim) pushFunding() {
	for _, sym := range exchange.Symbols {
		for _, e := range exchange.Exchanges {
			s.mu.Lock()
			// mean-reverting noise around the base rate
			s.fund[sym][e] += (baseFunding[sym] - s.fund[sym][e]) * 0.1
			s.fund[sym][e] *= 1 + (s.rng.Float64()-0.5)*0.1
			rate := s.fund[sym][e]
			s.mu.Unlock()
			s.feed.Funding(exchange.FundingInfo{Exchange: e, Sym: sym, Rate: rate, Time: time.Now()})
		}
	}
}

func (s *sim) pushOI() {
	for _, sym := range exchange.Symbols {
		for _, e := range exchange.Exchanges {
			s.mu.Lock()
			s.oi[sym][e] *= 1 + (s.rng.Float64()-0.5)*0.004
			val := s.oi[sym][e]
			s.mu.Unlock()
			s.feed.OI(exchange.OpenInterest{Exchange: e, Sym: sym, Value: val, Time: time.Now()})
		}
	}
}

func (s *sim) pushLS() {
	for _, sym := range exchange.Symbols {
		for _, e := range exchange.Exchanges {
			for _, p := range exchange.LSPeriods {
				drift := 0.0
				if p == "1d" {
					drift = (s.rng.Float64() - 0.5) * 2
				}
				longPct := baseLS[e] + drift + (s.rng.Float64()-0.5)*2.4
				s.feed.LS(exchange.LSRatio{
					Exchange: e, Sym: sym, Period: p,
					LongPct: longPct, ShortPct: 100 - longPct, Time: time.Now(),
				})
			}
		}
	}
}

func (s *sim) liqLoop(ctx context.Context) {
	weights := []struct {
		sym exchange.Symbol
		w   float64
	}{{exchange.BTC, 0.5}, {exchange.ETH, 0.3}, {exchange.SOL, 0.2}}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(1200+s.rng.Intn(2400)) * time.Millisecond):
		}
		r := s.rng.Float64()
		sym := exchange.BTC
		for _, w := range weights {
			if r < w.w {
				sym = w.sym
				break
			}
			r -= w.w
		}
		ex := exchange.Exchanges[s.rng.Intn(len(exchange.Exchanges))]
		side := "LONG"
		if s.rng.Float64() < 0.5 {
			side = "SHORT"
		}
		qty := baseQty[sym] * (0.15 + s.rng.ExpFloat64()*0.7)
		price := s.priceOf(sym) * (1 + (s.rng.Float64()-0.5)*0.002)
		s.feed.Liq(exchange.LiqEvent{
			Exchange: ex, Sym: sym, Side: side,
			Qty: qty, Price: price, USD: qty * price, Time: time.Now(),
		})
	}
}
