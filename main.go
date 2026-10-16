// Copyright (c) 2026 aziz-sudrajat <azizsudrajat.stb4@gmail.com>
// https://github.com/aziz-sudrajat
//
// All rights reserved. This file is part of the azzx-crypto-stream project.
// Unauthorized copying, modification, distribution, or false claim of
// authorship is prohibited.

// Command azzx-crypto-stream is a Coinglass-lite terminal: live multi-exchange
// futures data (prices, order books, liquidations, longs/shorts) in one Go binary.
// All data is public; no API key is needed anywhere.
package main

import (
	"context"
	"flag"
	"time"

	"github.com/aziz-sudrajat/azzx-crypto-stream/demo"
	"github.com/aziz-sudrajat/azzx-crypto-stream/exchange"
	"github.com/aziz-sudrajat/azzx-crypto-stream/store"
	"github.com/aziz-sudrajat/azzx-crypto-stream/tui"
	tea "github.com/charmbracelet/bubbletea"
)

// demoMode enables simulated market data for the --demo flag.
// demo mode — simulated data for README recording only, never used for trading decisions.
var demoMode = flag.Bool("demo", false, "demo mode: simulated data for README recording only, never used for trading decisions")

// venue is the common surface every exchange client implements.
type venue interface {
	Name() string
	Funding(context.Context, exchange.Symbol) (exchange.FundingInfo, error)
	OpenInterest(context.Context, exchange.Symbol) (exchange.OpenInterest, error)
	LongShortRatio(context.Context, exchange.Symbol, string) (exchange.LSRatio, error)
}

// uiSink forwards exchange events into the bubbletea program.
// bubbletea's Send is safe for concurrent use.
type uiSink struct {
	p *tea.Program
}

func (s uiSink) Ticker(t exchange.Ticker) {
	s.p.Send(tui.TickerMsg(t))
}
func (s uiSink) Book(b exchange.OrderBook) {
	s.p.Send(tui.BookMsg(b))
}
func (s uiSink) Liq(e exchange.LiqEvent) {
	s.p.Send(tui.LiqMsg(e))
}
func (s uiSink) Status(ex, state string) {
	s.p.Send(tui.StatusMsg{Exchange: ex, State: state})
}

// demoFeed adapts the demo simulator to bubbletea messages.
type demoFeed struct{ p *tea.Program }

func (d demoFeed) Ticker(t exchange.Ticker)       { d.p.Send(tui.TickerMsg(t)) }
func (d demoFeed) Book(b exchange.OrderBook)      { d.p.Send(tui.BookMsg(b)) }
func (d demoFeed) Liq(e exchange.LiqEvent)        { d.p.Send(tui.LiqMsg(e)) }
func (d demoFeed) Funding(f exchange.FundingInfo) { d.p.Send(tui.FundingMsg(f)) }
func (d demoFeed) OI(o exchange.OpenInterest)     { d.p.Send(tui.OIMsg(o)) }
func (d demoFeed) LS(r exchange.LSRatio)          { d.p.Send(tui.LSMsg(r)) }
func (d demoFeed) Status(ex, state string)        { d.p.Send(tui.StatusMsg{Exchange: ex, State: state}) }

func main() {
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st := store.NewLiqStore("liqstore.json")
	_ = st.Load() // missing file on first run is fine
	defer st.Save()

	model := tui.NewModel(st)
	program := tea.NewProgram(model, tea.WithAltScreen())
	sink := uiSink{p: program}

	binance := exchange.Binance{}
	bybit := exchange.NewBybit()
	okx := exchange.NewOKX()

	if *demoMode {
		// demo mode — simulated data for README recording only, never used for trading decisions.
		go demo.Run(ctx, demoFeed{p: program})
	} else {
		// --- websocket streams (fail-soft per venue, auto-reconnect) ---
		go binance.StreamTickers(ctx, exchange.Symbols, sink)
		go binance.StreamDepth(ctx, exchange.Symbols, sink)
		go binance.StreamLiquidations(ctx, sink)
		go bybit.StreamAll(ctx, exchange.Symbols, sink)
		go okx.StreamAll(ctx, exchange.Symbols, sink)
		// --- REST pollers ---
		venues := []venue{binance, bybit, okx}
		go pollLoop(ctx, 60*time.Second, func() {
			for _, v := range venues {
				for _, s := range exchange.Symbols {
					if f, err := v.Funding(ctx, s); err == nil {
						sink.p.Send(tui.FundingMsg(f))
					}
					if o, err := v.OpenInterest(ctx, s); err == nil {
						sink.p.Send(tui.OIMsg(o))
					}
				}
			}
		})
		go pollLoop(ctx, 5*time.Minute, func() {
			for _, v := range venues {
				for _, s := range exchange.Symbols {
					for _, p := range exchange.LSPeriods {
						if r, err := v.LongShortRatio(ctx, s, p); err == nil {
							sink.p.Send(tui.LSMsg(r))
						}
					}
				}
			}
		})
	}

	if _, err := program.Run(); err != nil {
		// bubbletea only errors on TTY problems; nothing to retry here.
		println("azzx-crypto-stream:", err.Error())
	}
}

// pollLoop runs fn immediately, then every interval, until ctx is cancelled.
func pollLoop(ctx context.Context, interval time.Duration, fn func()) {
	fn()
	t := time.NewTicker(interval)
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
