'use client';

import React, { useState, useRef, useCallback, useEffect } from 'react';
import { Search, X, AlertTriangle, ExternalLink } from 'lucide-react';

const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

interface SearchResult {
  asteroid_id: string;
  name: string;
  is_potentially_hazardous: boolean;
  estimated_diameter_km_max: number | null;
  source: string;
}

interface AsteroidSearchProps {
  onSelect: (designation: string) => void;
}

export default function AsteroidSearch({ onSelect }: AsteroidSearchProps) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const [activeIdx, setActiveIdx] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout>>(null);

  // Global keyboard shortcut: / to focus
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === '/' && !open && document.activeElement?.tagName !== 'INPUT') {
        e.preventDefault();
        setOpen(true);
        setTimeout(() => inputRef.current?.focus(), 50);
      }
      if (e.key === 'Escape') {
        setOpen(false);
        setQuery('');
        setResults([]);
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [open]);

  const doSearch = useCallback(async (q: string) => {
    if (q.length < 2) { setResults([]); return; }
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/search?q=${encodeURIComponent(q)}`);
      if (res.ok) {
        const data = await res.json();
        setResults(data.results || []);
        setActiveIdx(0);
      }
    } catch { /* ignore */ }
    setLoading(false);
  }, []);

  const handleChange = (val: string) => {
    setQuery(val);
    if (timerRef.current) clearTimeout(timerRef.current);
    timerRef.current = setTimeout(() => doSearch(val), 300);
  };

  const handleSelect = (r: SearchResult) => {
    onSelect(r.asteroid_id);
    setOpen(false);
    setQuery('');
    setResults([]);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') { e.preventDefault(); setActiveIdx(i => Math.min(i + 1, results.length - 1)); }
    if (e.key === 'ArrowUp') { e.preventDefault(); setActiveIdx(i => Math.max(i - 1, 0)); }
    if (e.key === 'Enter' && results[activeIdx]) { handleSelect(results[activeIdx]); }
  };

  if (!open) {
    return (
      <button
        onClick={() => { setOpen(true); setTimeout(() => inputRef.current?.focus(), 50); }}
        className="neo-search-trigger"
        title="Search asteroids ( / )"
      >
        <Search size={16} />
        <span>Search asteroids…</span>
        <kbd>/</kbd>
      </button>
    );
  }

  return (
    <div className="neo-search-overlay" onClick={() => { setOpen(false); setQuery(''); setResults([]); }}>
      <div className="neo-search-modal" onClick={e => e.stopPropagation()}>
        <div className="neo-search-input-row">
          <Search size={18} className="neo-search-icon" />
          <input
            ref={inputRef}
            type="text"
            placeholder="Search by name, designation, or ID…"
            value={query}
            onChange={e => handleChange(e.target.value)}
            onKeyDown={handleKeyDown}
            autoFocus
          />
          <button onClick={() => { setOpen(false); setQuery(''); setResults([]); }} className="neo-search-close">
            <X size={16} />
          </button>
        </div>
        {loading && <div className="neo-search-loading">Searching across agencies…</div>}
        {results.length > 0 && (
          <ul className="neo-search-results">
            {results.map((r, i) => (
              <li
                key={`${r.asteroid_id}-${i}`}
                className={`neo-search-item ${i === activeIdx ? 'active' : ''}`}
                onClick={() => handleSelect(r)}
                onMouseEnter={() => setActiveIdx(i)}
              >
                <div className="neo-search-item-left">
                  {r.is_potentially_hazardous && <AlertTriangle size={14} className="hazard-icon" />}
                  <div>
                    <span className="neo-search-name">{r.name}</span>
                    <span className="neo-search-id">{r.asteroid_id}</span>
                  </div>
                </div>
                <div className="neo-search-item-right">
                  <span className={`neo-search-source ${r.source === 'jpl_sbdb' ? 'jpl' : 'local'}`}>
                    {r.source === 'jpl_sbdb' ? 'JPL' : 'DB'}
                  </span>
                  <ExternalLink size={12} />
                </div>
              </li>
            ))}
          </ul>
        )}
        {!loading && query.length >= 2 && results.length === 0 && (
          <div className="neo-search-empty">No results found for &quot;{query}&quot;</div>
        )}
      </div>
    </div>
  );
}
