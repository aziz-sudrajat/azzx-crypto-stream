// Copyright (c) 2026 aziz-sudrajat <azizsudrajat.stb4@gmail.com>
// https://github.com/aziz-sudrajat
//
// All rights reserved. This file is part of the azzx-crypto-stream project.
// Unauthorized copying, modification, distribution, or false claim of
// authorship is prohibited.

package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/aziz-sudrajat/azzx-crypto-stream/exchange"
)

func TestLiqStoreTotals(t *testing.T) {
	s := NewLiqStore("")
	now := time.Now()
	s.Add(exchange.LiqEvent{Exchange: "Binance", Sym: exchange.BTC, Side: "LONG", USD: 100, Time: now})
	s.Add(exchange.LiqEvent{Exchange: "Binance", Sym: exchange.BTC, Side: "SHORT", USD: 50, Time: now})
	s.Add(exchange.LiqEvent{Exchange: "Bybit", Sym: exchange.ETH, Side: "LONG", USD: 25, Time: now})
	// stale event must be pruned
	s.Add(exchange.LiqEvent{Exchange: "Binance", Sym: exchange.BTC, Side: "LONG", USD: 9999, Time: now.Add(-25 * time.Hour)})

	totals := s.Totals24h()
	if len(totals) != 2 {
		t.Fatalf("expected 2 totals, got %d", len(totals))
	}
	if totals[0].Exchange != "Binance" || totals[0].LongUSD != 100 || totals[0].ShortUSD != 50 {
		t.Fatalf("bad binance total: %+v", totals[0])
	}
	if got := len(s.Recent(10)); got != 3 {
		t.Fatalf("expected 3 recent events after prune, got %d", got)
	}
}

func TestLiqStoreSaveLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "liq.json")
	s := NewLiqStore(path)
	now := time.Now().Truncate(time.Second)
	s.Add(exchange.LiqEvent{Exchange: "OKX", Sym: exchange.SOL, Side: "SHORT", Qty: 5, Price: 200, USD: 1000, Time: now})
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
	s2 := NewLiqStore(path)
	if err := s2.Load(); err != nil {
		t.Fatal(err)
	}
	got := s2.Recent(10)
	if len(got) != 1 || got[0].USD != 1000 || !got[0].Time.Equal(now) {
		t.Fatalf("bad restored event: %+v", got)
	}
}

func TestLiqStoreLoadMissing(t *testing.T) {
	s := NewLiqStore(filepath.Join(t.TempDir(), "nope.json"))
	if err := s.Load(); err != nil {
		t.Fatalf("missing file should not error: %v", err)
	}
}
