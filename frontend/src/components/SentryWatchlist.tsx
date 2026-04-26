'use client';

import React, { useState, useEffect } from 'react';
import { Shield, ChevronDown, ChevronUp } from 'lucide-react';

const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

interface SentryEntry {
  des: string;
  fullname?: string;
  ip: string;
  ps_cum?: string;
  ts_max?: string;
  diameter?: string;
  h?: string;
  last_obs?: string;
  n_imp?: number;
}

function torinoColor(ts: string): string {
  const t = parseInt(ts || '0');
  if (t === 0) return '#44cc44';
  if (t <= 1) return '#88cc00';
  if (t <= 4) return '#ffcc00';
  if (t <= 7) return '#ff8800';
  return '#ff2200';
}

interface SentryWatchlistProps {
  onSelectAsteroid: (designation: string) => void;
}

export default function SentryWatchlist({ onSelectAsteroid }: SentryWatchlistProps) {
  const [watchlist, setWatchlist] = useState<SentryEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [expanded, setExpanded] = useState(false);

  useEffect(() => {
    if (!expanded) return;
    if (watchlist.length > 0) return; // already fetched
    setLoading(true);
    fetch(`${API_BASE}/api/sentry/watchlist`)
      .then(r => r.json())
      .then(data => setWatchlist(data.watchlist || []))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [expanded, watchlist.length]);

  return (
    <div className="neo-sentry-widget">
      <button
        className="neo-sentry-toggle"
        onClick={() => setExpanded(e => !e)}
      >
        <Shield size={14} />
        <span>Sentry Watch</span>
        {expanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
      </button>

      {expanded && (
        <div className="neo-sentry-list">
          {loading ? (
            <div className="neo-sentry-loading">Loading impact data…</div>
          ) : (
            watchlist.slice(0, 30).map((entry, i) => (
              <div
                key={`${entry.des}-${i}`}
                className="neo-sentry-item"
                onClick={() => onSelectAsteroid(entry.des)}
              >
                <div
                  className="neo-torino-dot"
                  style={{ backgroundColor: torinoColor(entry.ts_max || '0') }}
                />
                <div className="neo-sentry-info">
                  <span className="neo-sentry-name">{entry.fullname || entry.des}</span>
                  <span className="neo-sentry-ip">IP: {entry.ip}</span>
                </div>
                <span className="neo-sentry-ts" style={{ color: torinoColor(entry.ts_max || '0') }}>
                  T{entry.ts_max || '0'}
                </span>
              </div>
            ))
          )}
        </div>
      )}
    </div>
  );
}
