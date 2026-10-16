// Copyright (c) 2026 aziz-sudrajat <azizsudrajat.stb4@gmail.com>
// https://github.com/aziz-sudrajat
//
// All rights reserved. This file is part of the azzx-crypto-stream project.
// Unauthorized copying, modification, distribution, or false claim of
// authorship is prohibited.

package demo

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/aziz-sudrajat/azzx-crypto-stream/exchange"
)

func newTestRng() *rand.Rand { return rand.New(rand.NewSource(1)) }

type testFeed struct {
	mu       sync.Mutex
	tickers  int
	books    int
	liqs     int
	fundings int
	ois      int
	lss      int
	statusOK bool
}

func (f *testFeed) Ticker(exchange.Ticker)       { f.mu.Lock(); f.tickers++; f.mu.Unlock() }
func (f *testFeed) Book(exchange.OrderBook)      { f.mu.Lock(); f.books++; f.mu.Unlock() }
func (f *testFeed) Liq(exchange.LiqEvent)        { f.mu.Lock(); f.liqs++; f.mu.Unlock() }
func (f *testFeed) Funding(exchange.FundingInfo) { f.mu.Lock(); f.fundings++; f.mu.Unlock() }
func (f *testFeed) OI(exchange.OpenInterest)     { f.mu.Lock(); f.ois++; f.mu.Unlock() }
func (f *testFeed) LS(exchange.LSRatio)          { f.mu.Lock(); f.lss++; f.mu.Unlock() }
func (f *testFeed) Status(_ string, s string) {
	f.mu.Lock()
	f.statusOK = f.statusOK || s == "connected"
	f.mu.Unlock()
}

// TestDemoRun ensures the simulator produces every data kind headlessly.
func TestDemoRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	feed := &testFeed{}
	done := make(chan struct{})
	go func() { Run(ctx, feed); close(done) }()
	<-done
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.tickers == 0 {
		t.Error("no tickers produced")
	}
	if feed.books == 0 {
		t.Error("no order books produced")
	}
	if feed.fundings == 0 || feed.ois == 0 {
		t.Error("no funding/OI produced")
	}
	if feed.lss == 0 {
		t.Error("no L/S ratios produced")
	}
	if !feed.statusOK {
		t.Error("no connected status produced")
	}
	// liq events arrive every 1.2-3.6s; 3s run should usually see one
	t.Logf("tickers=%d books=%d liqs=%d fundings=%d ois=%d ls=%d",
		feed.tickers, feed.books, feed.liqs, feed.fundings, feed.ois, feed.lss)
}

func TestMakeBook(t *testing.T) {
	rng := newTestRng()
	ob := makeBook(exchange.BinanceName, exchange.BTC, 97412.5, rng)
	if len(ob.Bids) != 20 || len(ob.Asks) != 20 {
		t.Fatalf("expected 20 levels each, got %d/%d", len(ob.Bids), len(ob.Asks))
	}
	if ob.Bids[0].Price >= ob.Asks[0].Price {
		t.Fatalf("crossed book: best bid %f >= best ask %f", ob.Bids[0].Price, ob.Asks[0].Price)
	}
	for i := 1; i < len(ob.Bids); i++ {
		if ob.Bids[i].Price >= ob.Bids[i-1].Price {
			t.Fatalf("bids not descending at %d", i)
		}
	}
	for i := 1; i < len(ob.Asks); i++ {
		if ob.Asks[i].Price <= ob.Asks[i-1].Price {
			t.Fatalf("asks not ascending at %d", i)
		}
	}
}
