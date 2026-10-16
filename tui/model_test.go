// Copyright (c) 2026 aziz-sudrajat <azizsudrajat.stb4@gmail.com>
// https://github.com/aziz-sudrajat
//
// All rights reserved. This file is part of the azzx-crypto-stream project.
// Unauthorized copying, modification, distribution, or false claim of
// authorship is prohibited.

package tui

import (
	"strings"
	"testing"

	"github.com/aziz-sudrajat/azzx-crypto-stream/exchange"
	"github.com/aziz-sudrajat/azzx-crypto-stream/store"
	tea "github.com/charmbracelet/bubbletea"
)

// Tickers/funding/OI for different symbols on the same exchange must not
// overwrite each other: the Overview tab shows the current symbol, and all
// three symbols are subscribed per exchange.
func TestMultiSymbolKeying(t *testing.T) {
	m := NewModel(store.NewLiqStore(""))

	// Simulate one update batch per symbol, same order the live/demo feeds use.
	for _, sym := range []exchange.Symbol{exchange.BTC, exchange.ETH, exchange.SOL} {
		price := 1000.0
		if sym == exchange.BTC {
			price = 97000.0
		}
		if _, cmd := m.Update(TickerMsg(exchange.Ticker{Exchange: exchange.BinanceName, Sym: sym, Price: price})); cmd != nil {
			_ = cmd
		}
		if _, cmd := m.Update(FundingMsg(exchange.FundingInfo{Exchange: exchange.BinanceName, Sym: sym, Rate: 0.0001})); cmd != nil {
			_ = cmd
		}
		if _, cmd := m.Update(OIMsg(exchange.OpenInterest{Exchange: exchange.BinanceName, Sym: sym, Value: 123456})); cmd != nil {
			_ = cmd
		}
	}

	if len(m.tickers) != 3 {
		t.Fatalf("expected 3 tickers keyed by exchange|symbol, got %d", len(m.tickers))
	}

	// Overview for BTC must show the BTC price, not SOL's (last write wins bug).
	m.sym = exchange.BTC
	m.tab = tabOverview
	view := m.View()
	if !strings.Contains(view, "97000.0") {
		t.Errorf("overview for BTC should show 97000.0 price, got view:\n%s", view)
	}

	// Switching symbol shows that symbol's data.
	m.sym = exchange.SOL
	view = m.View()
	if !strings.Contains(view, "1000.0") {
		t.Errorf("overview for SOL should show 1000.0 price, got view:\n%s", view)
	}
}

// Long/short ratios are also keyed by exchange|symbol.
func TestLongShortMultiSymbol(t *testing.T) {
	m := NewModel(store.NewLiqStore(""))
	for _, sym := range []exchange.Symbol{exchange.BTC, exchange.ETH, exchange.SOL} {
		longPct := 60.0
		if sym == exchange.ETH {
			longPct = 42.0
		}
		if _, cmd := m.Update(LSMsg(exchange.LSRatio{
			Exchange: exchange.BybitName, Sym: sym, Period: "4h",
			LongPct: longPct, ShortPct: 100 - longPct,
		})); cmd != nil {
			_ = cmd
		}
	}
	m.sym = exchange.ETH
	m.tab = tabLongShort
	view := m.View()
	if !strings.Contains(view, "42.0%") {
		t.Errorf("longs/shorts for ETH should show 42.0%%, got view:\n%s", view)
	}
}

// Tab switching via keys 1-4 works.
func TestTabKeys(t *testing.T) {
	m := NewModel(store.NewLiqStore(""))
	for key, want := range map[string]int{"1": tabOverview, "2": tabOrderBook, "3": tabLiquidations, "4": tabLongShort} {
		updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(key)})
		if updated.(*Model).tab != want {
			t.Errorf("key %q: expected tab %d, got %d", key, want, updated.(*Model).tab)
		}
	}
}
