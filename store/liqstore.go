// Copyright (c) 2026 aziz-sudrajat <azizsudrajat.stb4@gmail.com>
// https://github.com/aziz-sudrajat
//
// All rights reserved. This file is part of the azzx-crypto-stream project.
// Unauthorized copying, modification, distribution, or false claim of
// authorship is prohibited.

// Package store accumulates liquidation events into rolling 24h totals with
// JSON persistence, so per-exchange liquidation tables survive restarts.
package store

import (
	"encoding/json"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/aziz-sudrajat/azzx-crypto-stream/exchange"
)

const window = 24 * time.Hour

// LiqStore is a thread-safe rolling window of liquidation events.
type LiqStore struct {
	mu     sync.Mutex
	events []exchange.LiqEvent
	path   string
}

// NewLiqStore creates a store persisting to path (empty path = no persistence).
func NewLiqStore(path string) *LiqStore {
	return &LiqStore{path: path}
}

// Add inserts an event and prunes anything older than 24h.
func (s *LiqStore) Add(e exchange.LiqEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	cutoff := time.Now().Add(-window)
	kept := s.events[:0]
	for _, ev := range s.events {
		if ev.Time.After(cutoff) {
			kept = append(kept, ev)
		}
	}
	s.events = kept
}

// Recent returns the n most recent events, newest first.
func (s *LiqStore) Recent(n int) []exchange.LiqEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]exchange.LiqEvent, len(s.events))
	copy(out, s.events)
	sort.Slice(out, func(i, j int) bool { return out[i].Time.After(out[j].Time) })
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// Total is one row of the per-exchange 24h liquidation table.
type Total struct {
	Exchange string
	Sym      exchange.Symbol
	LongUSD  float64
	ShortUSD float64
}

// Totals24h aggregates USD liquidated per exchange+symbol over the window.
func (s *LiqStore) Totals24h() []Total {
	s.mu.Lock()
	defer s.mu.Unlock()
	byKey := map[[2]string]*Total{}
	for _, e := range s.events {
		k := [2]string{e.Exchange, string(e.Sym)}
		t, ok := byKey[k]
		if !ok {
			t = &Total{Exchange: e.Exchange, Sym: e.Sym}
			byKey[k] = t
		}
		if e.Side == "LONG" {
			t.LongUSD += e.USD
		} else {
			t.ShortUSD += e.USD
		}
	}
	out := make([]Total, 0, len(byKey))
	for _, t := range byKey {
		out = append(out, *t)
	}
	sort.Slice(out, func(i, j int) bool {
		ti := out[i].LongUSD + out[i].ShortUSD
		tj := out[j].LongUSD + out[j].ShortUSD
		return ti > tj
	})
	return out
}

// Save persists events to disk. No-op when path is empty.
func (s *LiqStore) Save() error {
	if s.path == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(s.events)
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0o600)
}

// Load restores events from disk. Missing file is not an error.
func (s *LiqStore) Load() error {
	if s.path == "" {
		return nil
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var events []exchange.LiqEvent
	if err := json.Unmarshal(data, &events); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = events
	return nil
}
