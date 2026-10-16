// Copyright (c) 2026 aziz-sudrajat <azizsudrajat.stb4@gmail.com>
// https://github.com/aziz-sudrajat
//
// All rights reserved. This file is part of the azzx-crypto-stream project.
// Unauthorized copying, modification, distribution, or false claim of
// authorship is prohibited.

// Package tui implements the bubbletea terminal UI: four live tabs over
// multi-exchange futures data.
package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/aziz-sudrajat/azzx-crypto-stream/exchange"
	"github.com/aziz-sudrajat/azzx-crypto-stream/store"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// --- messages ---------------------------------------------------------------

// TickerMsg carries a live price tick.
type TickerMsg exchange.Ticker

// FundingMsg carries a funding-rate snapshot.
type FundingMsg exchange.FundingInfo

// OIMsg carries an open-interest snapshot.
type OIMsg exchange.OpenInterest

// BookMsg carries an order-book snapshot.
type BookMsg exchange.OrderBook

// LiqMsg carries one liquidation event.
type LiqMsg exchange.LiqEvent

// LSMsg carries a long/short ratio snapshot.
type LSMsg exchange.LSRatio

// StatusMsg reports a venue connection state.
type StatusMsg struct {
	Exchange string
	State    string
}

type refreshMsg time.Time

// --- styles -----------------------------------------------------------------

var (
	cGreen     = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	cRed       = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	cDim       = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	cYellow    = lipgloss.NewStyle().Foreground(lipgloss.Color("11"))
	cBold      = lipgloss.NewStyle().Bold(true)
	cActiveTab = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("14")).Underline(true)
)

const (
	tabOverview = iota
	tabOrderBook
	tabLiquidations
	tabLongShort
)

var tabNames = []string{"Overview", "OrderBook", "Liquidations", "Longs/Shorts"}

// --- model ------------------------------------------------------------------

// Model is the bubbletea root model.
type Model struct {
	store    *store.LiqStore
	sym      exchange.Symbol
	tab      int
	tickers  map[string]exchange.Ticker
	funding  map[string]exchange.FundingInfo
	oi       map[string]exchange.OpenInterest
	books    map[string]exchange.OrderBook // key: exchange|sym
	bookEx   string
	liqs     []exchange.LiqEvent
	ls       map[string]map[string]exchange.LSRatio // period -> exchange -> ratio
	lsPeriod string
	status   map[string]string
	width    int
	now      time.Time
}

// NewModel builds the root model.
func NewModel(st *store.LiqStore) *Model {
	return &Model{
		store:    st,
		sym:      exchange.BTC,
		books:    make(map[string]exchange.OrderBook),
		bookEx:   exchange.BinanceName,
		tickers:  make(map[string]exchange.Ticker),
		funding:  make(map[string]exchange.FundingInfo),
		oi:       make(map[string]exchange.OpenInterest),
		ls:       make(map[string]map[string]exchange.LSRatio),
		lsPeriod: "4h",
		status:   map[string]string{exchange.BinanceName: "connecting", exchange.BybitName: "connecting", exchange.OKXName: "connecting"},
		liqs:     st.Recent(20),
		width:    100,
		now:      time.Now(),
	}
}

// Init starts the periodic UI refresh tick.
func (m *Model) Init() tea.Cmd {
	return tea.Tick(500*time.Millisecond, func(t time.Time) tea.Msg { return refreshMsg(t) })
}

// Update routes messages into state.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		return m, nil
	case refreshMsg:
		m.now = time.Time(msg)
		return m, tea.Tick(500*time.Millisecond, func(t time.Time) tea.Msg { return refreshMsg(t) })
	case tea.KeyMsg:
		return m.handleKey(msg)
	case TickerMsg:
		t := exchange.Ticker(msg)
		m.tickers[t.Exchange+"|"+string(t.Sym)] = t
	case FundingMsg:
		f := exchange.FundingInfo(msg)
		m.funding[f.Exchange+"|"+string(f.Sym)] = f
	case OIMsg:
		o := exchange.OpenInterest(msg)
		m.oi[o.Exchange+"|"+string(o.Sym)] = o
	case BookMsg:
		b := exchange.OrderBook(msg)
		m.books[b.Exchange+"|"+string(b.Sym)] = b
	case LiqMsg:
		e := exchange.LiqEvent(msg)
		m.store.Add(e)
		m.liqs = append([]exchange.LiqEvent{e}, m.liqs...)
		if len(m.liqs) > 30 {
			m.liqs = m.liqs[:30]
		}
	case LSMsg:
		r := exchange.LSRatio(msg)
		if m.ls[r.Period] == nil {
			m.ls[r.Period] = make(map[string]exchange.LSRatio)
		}
		m.ls[r.Period][r.Exchange+"|"+string(r.Sym)] = r
	case StatusMsg:
		m.status[msg.Exchange] = msg.State
	}
	return m, nil
}

func (m *Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c":
		return m, tea.Quit
	case "1":
		m.tab = tabOverview
	case "2":
		m.tab = tabOrderBook
	case "3":
		m.tab = tabLiquidations
	case "4":
		m.tab = tabLongShort
	case "s":
		m.cycleSymbol()
	case "e":
		if m.tab == tabOrderBook {
			m.cycleBookExchange()
		}
	case "f":
		if m.tab == tabLongShort {
			for i, p := range exchange.LSPeriods {
				if p == m.lsPeriod {
					m.lsPeriod = exchange.LSPeriods[(i+1)%len(exchange.LSPeriods)]
					break
				}
			}
		}
	}
	return m, nil
}

func (m *Model) cycleSymbol() {
	for i, s := range exchange.Symbols {
		if s == m.sym {
			m.sym = exchange.Symbols[(i+1)%len(exchange.Symbols)]
			return
		}
	}
}

func (m *Model) cycleBookExchange() {
	for i, e := range exchange.Exchanges {
		if e == m.bookEx {
			m.bookEx = exchange.Exchanges[(i+1)%len(exchange.Exchanges)]
			return
		}
	}
}

// --- view -------------------------------------------------------------------

// View renders the whole screen.
func (m *Model) View() string {
	var b strings.Builder
	b.WriteString(m.header())
	b.WriteString("\n")
	b.WriteString(m.tabBar())
	b.WriteString("\n\n")
	switch m.tab {
	case tabOverview:
		b.WriteString(m.viewOverview())
	case tabOrderBook:
		b.WriteString(m.viewOrderBook())
	case tabLiquidations:
		b.WriteString(m.viewLiquidations())
	case tabLongShort:
		b.WriteString(m.viewLongShort())
	}
	b.WriteString("\n\n")
	b.WriteString(m.footer())
	return b.String()
}

func (m *Model) header() string {
	var syms []string
	for _, s := range exchange.Symbols {
		if s == m.sym {
			syms = append(syms, cActiveTab.Render(string(s)))
		} else {
			syms = append(syms, cDim.Render(string(s)))
		}
	}
	var dots []string
	for _, e := range exchange.Exchanges {
		st := m.status[e]
		dot := cGreen.Render("●")
		if strings.HasPrefix(st, "error") || strings.HasPrefix(st, "reconnecting") {
			dot = cRed.Render("●")
		} else if st != "connected" {
			dot = cYellow.Render("●")
		}
		dots = append(dots, fmt.Sprintf("%s %s", dot, e))
	}
	return fmt.Sprintf("%s  %s   %s   %s WIB",
		cBold.Render("azzx-crypto-stream"),
		strings.Join(syms, " "),
		strings.Join(dots, "  "),
		m.now.Format("15:04:05"),
	)
}

func (m *Model) tabBar() string {
	var tabs []string
	for i, name := range tabNames {
		if i == m.tab {
			tabs = append(tabs, cActiveTab.Render(fmt.Sprintf("[%d] %s", i+1, name)))
		} else {
			tabs = append(tabs, cDim.Render(fmt.Sprintf("[%d] %s", i+1, name)))
		}
	}
	return strings.Join(tabs, "   ")
}

func (m *Model) footer() string {
	extra := "s: ganti simbol   q: keluar"
	if m.tab == tabOrderBook {
		extra = "e: ganti exchange   " + extra
	}
	if m.tab == tabLongShort {
		extra = "f: ganti timeframe (4h/1d)   " + extra
	}
	return cDim.Render(extra)
}

// --- tab: overview ------------------------------------------------------------

func (m *Model) viewOverview() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-10s %-14s %-12s %-16s %s\n", "EXCHANGE", "PRICE", "FUNDING", "OPEN INTEREST", "STATUS")
	for _, e := range exchange.Exchanges {
		key := e + "|" + string(m.sym)
		price := "—"
		if t, ok := m.tickers[key]; ok {
			price = fmtPrice(t.Price)
		}
		fund := "—"
		if f, ok := m.funding[key]; ok {
			fs := fmt.Sprintf("%+.4f%%", f.Rate*100)
			if f.Rate >= 0 {
				fund = cGreen.Render(fs)
			} else {
				fund = cRed.Render(fs)
			}
		}
		oi := "—"
		if o, ok := m.oi[key]; ok {
			oi = "$" + fmtUSD(o.Value)
		}
		st := m.status[e]
		if st != "connected" {
			st = cRed.Render(st)
		} else {
			st = cGreen.Render("live")
		}
		fmt.Fprintf(&b, "%-10s %-14s %-12s %-16s %s\n", e, price, fund, oi, st)
	}
	return b.String()
}

// --- tab: order book ------------------------------------------------------------

func (m *Model) viewOrderBook() string {
	ob, ok := m.books[m.bookEx+"|"+string(m.sym)]
	if !ok || len(ob.Bids) == 0 || len(ob.Asks) == 0 {
		return cDim.Render(fmt.Sprintf("Menunggu order book %s dari %s …", m.sym, m.bookEx))
	}
	const n = 10
	bids, asks := ob.Bids, ob.Asks
	if len(bids) > n {
		bids = bids[:n]
	}
	if len(asks) > n {
		asks = asks[:n]
	}
	// cumulative depth for bars
	maxCum := 0.0
	cumAsks := make([]float64, len(asks))
	cum := 0.0
	for i := len(asks) - 1; i >= 0; i-- { // from best ask outward
		cum += asks[i].Qty
		cumAsks[i] = cum
	}
	cumBids := make([]float64, len(bids))
	cum = 0.0
	for i := range bids {
		cum += bids[i].Qty
		cumBids[i] = cum
	}
	for _, c := range cumAsks {
		if c > maxCum {
			maxCum = c
		}
	}
	for _, c := range cumBids {
		if c > maxCum {
			maxCum = c
		}
	}
	barW := 24
	if m.width < 80 {
		barW = 12
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s  %s\n", cBold.Render(string(m.sym)+" OrderBook"), cDim.Render(m.bookEx))
	fmt.Fprintf(&b, "%-12s %-*s %12s %14s\n", "HARGA", barW, "DEPTH", "UKURAN", "KUMULATIF")
	for i := len(asks) - 1; i >= 0; i-- { // print worst->best so best ask sits at spread
		a := asks[i]
		bar := strings.Repeat("█", int(cumAsks[i]/maxCum*float64(barW)))
		fmt.Fprintf(&b, "%s %-*s %12s %14s\n",
			cRed.Render(fmt.Sprintf("%-12s", fmtPrice(a.Price))),
			barW, cRed.Render(bar),
			fmtQty(a.Qty), fmtQty(cumAsks[i]))
	}
	spread := asks[0].Price - bids[0].Price
	fmt.Fprintf(&b, "%s\n", cDim.Render(fmt.Sprintf("── spread %s ──", fmtPrice(spread))))
	for i := range bids {
		bd := bids[i]
		bar := strings.Repeat("█", int(cumBids[i]/maxCum*float64(barW)))
		fmt.Fprintf(&b, "%s %-*s %12s %14s\n",
			cGreen.Render(fmt.Sprintf("%-12s", fmtPrice(bd.Price))),
			barW, cGreen.Render(bar),
			fmtQty(bd.Qty), fmtQty(cumBids[i]))
	}
	return b.String()
}

// --- tab: liquidations ------------------------------------------------------------

func (m *Model) viewLiquidations() string {
	var b strings.Builder
	b.WriteString(cBold.Render("Feed likuidasi terbaru") + "\n")
	fmt.Fprintf(&b, "%-9s %-9s %-6s %-7s %-10s %-12s %s\n", "WAKTU", "EXCHANGE", "SYM", "SIDE", "QTY", "USD", "HARGA")
	shown := 0
	for _, e := range m.liqs {
		if shown >= 15 {
			break
		}
		shown++
		side := cGreen.Render("LONG")
		if e.Side == "SHORT" {
			side = cRed.Render("SHORT")
		}
		fmt.Fprintf(&b, "%-9s %-9s %-6s %-7s %-10s %-12s %s\n",
			e.Time.Format("15:04:05"), e.Exchange, e.Sym, side,
			fmtQty(e.Qty), "$"+fmtUSD(e.USD), fmtPrice(e.Price))
	}
	if shown == 0 {
		b.WriteString(cDim.Render("Belum ada event — feed berjalan, tunggu likuidasi terjadi…\n"))
	}
	b.WriteString("\n" + cBold.Render("Total 24 jam per exchange") + "\n")
	fmt.Fprintf(&b, "%-9s %-6s %-14s %-14s %-14s\n", "EXCHANGE", "SYM", "LONG LIQ", "SHORT LIQ", "TOTAL")
	for _, t := range m.store.Totals24h() {
		fmt.Fprintf(&b, "%-9s %-6s %-14s %-14s %-14s\n",
			t.Exchange, t.Sym,
			cGreen.Render("$"+fmtUSD(t.LongUSD)),
			cRed.Render("$"+fmtUSD(t.ShortUSD)),
			"$"+fmtUSD(t.LongUSD+t.ShortUSD))
	}
	return b.String()
}

// --- tab: longs/shorts ------------------------------------------------------------

func (m *Model) viewLongShort() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s  %s\n\n", cBold.Render("Longs/Shorts"), cDim.Render("timeframe: "+m.lsPeriod+" (f untuk ganti)"))
	ratios, ok := m.ls[m.lsPeriod]
	if !ok {
		return b.String() + cDim.Render("Memuat rasio…")
	}
	barW := 30
	if m.width < 80 {
		barW = 16
	}
	fmt.Fprintf(&b, "%-10s %-*s %7s %7s\n", "BURSA", barW+2, "", "LONG", "SHORT")
	for _, e := range exchange.Exchanges {
		r, ok := ratios[e+"|"+string(m.sym)]
		if !ok {
			fmt.Fprintf(&b, "%-10s %s\n", e, cDim.Render("—"))
			continue
		}
		lw := int(r.LongPct / 100 * float64(barW))
		sw := barW - lw
		bar := cGreen.Render(strings.Repeat("█", lw)) + cRed.Render(strings.Repeat("█", sw))
		fmt.Fprintf(&b, "%-10s %s %6.1f%% %6.1f%%\n", e, bar, r.LongPct, r.ShortPct)
	}
	return b.String()
}

// --- formatting helpers ------------------------------------------------------------

func fmtPrice(p float64) string {
	switch {
	case p >= 1000:
		return fmt.Sprintf("%.1f", p)
	case p >= 100:
		return fmt.Sprintf("%.2f", p)
	case p >= 1:
		return fmt.Sprintf("%.3f", p)
	default:
		return fmt.Sprintf("%.5f", p)
	}
}

func fmtQty(q float64) string {
	if q >= 1000 {
		return fmt.Sprintf("%.1f", q)
	}
	return fmt.Sprintf("%.3f", q)
}

func fmtUSD(v float64) string {
	switch {
	case v >= 1e9:
		return fmt.Sprintf("%.2fB", v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("%.2fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.1fK", v/1e3)
	default:
		return fmt.Sprintf("%.0f", v)
	}
}
