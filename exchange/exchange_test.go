// Copyright (c) 2026 aziz-sudrajat <azizsudrajat.stb4@gmail.com>
// https://github.com/aziz-sudrajat
//
// All rights reserved. This file is part of the azzx-crypto-stream project.
// Unauthorized copying, modification, distribution, or false claim of
// authorship is prohibited.

package exchange

import (
	"math"
	"testing"
)

func TestPairUnpair(t *testing.T) {
	cases := []struct {
		exchange string
		sym      Symbol
		market   string
	}{
		{BinanceName, BTC, "BTCUSDT"},
		{BybitName, ETH, "ETHUSDT"},
		{OKXName, SOL, "SOL-USDT-SWAP"},
		{OKXName, BTC, "BTC-USDT-SWAP"},
	}
	for _, c := range cases {
		if got := Pair(c.exchange, c.sym); got != c.market {
			t.Errorf("Pair(%s,%s) = %s, want %s", c.exchange, c.sym, got, c.market)
		}
		if sym, ok := Unpair(c.exchange, c.market); !ok || sym != c.sym {
			t.Errorf("Unpair(%s,%s) = %s,%v, want %s,true", c.exchange, c.market, sym, ok, c.sym)
		}
	}
	if _, ok := Unpair(BinanceName, "DOGEUSDT"); ok {
		t.Error("Unpair should reject unsupported markets")
	}
}

func TestParseBinanceMiniTicker(t *testing.T) {
	msg := []byte(`{"e":"24hrMiniTicker","E":123,"s":"BTCUSDT","c":"97412.5","o":"96000","h":"98000","l":"95500","v":"1234","q":"119M"}`)
	tk, err := parseBinanceMiniTicker(msg)
	if err != nil {
		t.Fatal(err)
	}
	if tk.Exchange != BinanceName || tk.Sym != BTC || tk.Price != 97412.5 {
		t.Fatalf("bad ticker: %+v", tk)
	}
	// subscribe ack must be ignored, not crash
	if _, err := parseBinanceMiniTicker([]byte(`{"result":null,"id":1}`)); err == nil {
		t.Error("expected error for subscribe ack")
	}
}

func TestParseBinanceDepth(t *testing.T) {
	msg := []byte(`{"e":"depthUpdate","E":123,"s":"ETHUSDT","b":[["3412.10","5.5"],["3412.00","3.1"]],"a":[["3412.44","2.2"],["3412.50","7.7"]]}`)
	ob, err := parseBinanceDepth(msg)
	if err != nil {
		t.Fatal(err)
	}
	if ob.Sym != ETH || len(ob.Bids) != 2 || len(ob.Asks) != 2 {
		t.Fatalf("bad book: %+v", ob)
	}
	if ob.Bids[0].Price != 3412.10 || ob.Asks[0].Price != 3412.44 {
		t.Fatalf("book not sorted: %+v", ob)
	}
}

func TestParseBinanceForceOrders(t *testing.T) {
	msg := []byte(`{"e":"forceOrder","E":123,"o":[{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC","q":"0.500","p":"97000.0","ap":"96990","X":"FILLED","l":"0.5","z":"0.5","T":1729000000000},{"s":"DOGEUSDT","S":"BUY","q":"100","p":"0.32","T":1729000001000}]}`)
	evs := parseBinanceForceOrders(msg)
	if len(evs) != 1 { // DOGEUSDT unsupported -> skipped
		t.Fatalf("expected 1 event, got %d", len(evs))
	}
	e := evs[0]
	if e.Side != "LONG" || e.Sym != BTC || math.Abs(e.USD-48500) > 0.01 {
		t.Fatalf("bad liq event: %+v", e)
	}
}

func TestParseBybitTicker(t *testing.T) {
	msg := []byte(`{"topic":"tickers.BTCUSDT","type":"snapshot","data":{"symbol":"BTCUSDT","lastPrice":"97410.2","fundingRate":"0.0001"},"ts":123}`)
	tk, err := parseBybitTicker(msg)
	if err != nil {
		t.Fatal(err)
	}
	if tk.Exchange != BybitName || tk.Sym != BTC || tk.Price != 97410.2 {
		t.Fatalf("bad ticker: %+v", tk)
	}
}

func TestBybitBookSnapshotAndDelta(t *testing.T) {
	b := NewBybit()
	snap := []byte(`{"topic":"orderbook.50.ETHUSDT","type":"snapshot","data":{"s":"ETHUSDT","b":[["3412.10","5.5"],["3412.00","3.0"]],"a":[["3412.44","2.2"]],"u":1,"seq":1},"ts":123}`)
	ob, ok := b.applyBookMsg(snap)
	if !ok || len(ob.Bids) != 2 || ob.Sym != ETH {
		t.Fatalf("bad snapshot: %+v %v", ob, ok)
	}
	delta := []byte(`{"topic":"orderbook.50.ETHUSDT","type":"delta","data":{"s":"ETHUSDT","b":[["3412.10","0"],["3411.90","9.9"]],"a":[],"u":2,"seq":2},"ts":124}`)
	ob, ok = b.applyBookMsg(delta)
	if !ok {
		t.Fatal("delta not applied")
	}
	// 3412.10 removed (qty 0), 3411.90 added -> best bid now 3412.00
	if len(ob.Bids) != 2 || ob.Bids[0].Price != 3412.00 || ob.Bids[1].Qty != 9.9 {
		t.Fatalf("bad delta book: %+v", ob.Bids)
	}
}

func TestParseBybitLiquidation(t *testing.T) {
	msg := []byte(`{"topic":"allLiquidation.SOLUSDT","type":"snapshot","data":[{"T":1729000000000,"s":"SOLUSDT","S":"Sell","v":"10.5","p":"214.30"}],"ts":123}`)
	evs := parseBybitLiquidation(msg)
	if len(evs) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evs))
	}
	e := evs[0]
	if e.Side != "LONG" || e.Sym != SOL || math.Abs(e.USD-2250.15) > 0.01 {
		t.Fatalf("bad liq event: %+v", e)
	}
}

func TestParseOKXTicker(t *testing.T) {
	msg := []byte(`{"arg":{"channel":"tickers","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","last":"97415.8","ts":"1729000000000"}]}`)
	tk, err := parseOKXTicker("BTC-USDT-SWAP", msg)
	if err != nil {
		t.Fatal(err)
	}
	if tk.Exchange != OKXName || tk.Sym != BTC || tk.Price != 97415.8 {
		t.Fatalf("bad ticker: %+v", tk)
	}
	if _, err := parseOKXTicker("DOGE-USDT-SWAP", msg); err == nil {
		t.Error("expected error for unsupported instId")
	}
}

func TestOKXBookSnapshotAndUpdate(t *testing.T) {
	o := NewOKX()
	snap := []byte(`{"arg":{"channel":"books","instId":"ETH-USDT-SWAP"},"action":"snapshot","data":[{"asks":[["3412.44","2.2","0","1"]],"bids":[["3412.10","5.5","0","1"],["3412.00","3.0","0","2"]],"ts":"1","checksum":0}]}`)
	ob, ok := o.applyBookMsg("ETH-USDT-SWAP", snap)
	if !ok || len(ob.Bids) != 2 || ob.Sym != ETH {
		t.Fatalf("bad snapshot: %+v %v", ob, ok)
	}
	upd := []byte(`{"arg":{"channel":"books","instId":"ETH-USDT-SWAP"},"action":"update","data":[{"asks":[],"bids":[["3412.10","0","0","0"]],"ts":"2","checksum":0}]}`)
	ob, ok = o.applyBookMsg("ETH-USDT-SWAP", upd)
	if !ok || len(ob.Bids) != 1 || ob.Bids[0].Price != 3412.00 {
		t.Fatalf("bad update book: %+v", ob.Bids)
	}
}

func TestParseOKXLiquidations(t *testing.T) {
	msg := []byte(`{"arg":{"channel":"liquidation-orders","instType":"SWAP"},"data":[{"instId":"BTC-USDT-SWAP","px":"97000","sz":"0.5","side":"sell","posSide":"long","cTime":"1729000000000"}]}`)
	evs := parseOKXLiquidations(msg)
	if len(evs) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evs))
	}
	e := evs[0]
	if e.Side != "LONG" || e.Sym != BTC || math.Abs(e.USD-48500) > 0.01 {
		t.Fatalf("bad liq event: %+v", e)
	}
}

func TestBackoff(t *testing.T) {
	if Backoff(0) != 1_000_000_000 {
		t.Errorf("Backoff(0) = %v, want 1s", Backoff(0))
	}
	if Backoff(99) != 30_000_000_000 {
		t.Errorf("Backoff(99) = %v, want 30s cap", Backoff(99))
	}
}
