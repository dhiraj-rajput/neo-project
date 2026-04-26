'use client';
import React, { useState, useEffect, useCallback } from 'react';
import { format, addDays, subDays } from 'date-fns';
import dynamic from 'next/dynamic';
import type { AsteroidData } from '@/components/SolarSystem';
import './page.css';

// Lazy-load the 3D canvas so Next.js doesn't try to SSR Three.js
const SolarSystem = dynamic(() => import('@/components/SolarSystem'), {
  ssr: false,
  loading: () => (
    <div className="loading-overlay">
      <div className="loading-text font-mono">INITIALIZING 3D ENGINE...</div>
    </div>
  ),
});

const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export default function Home() {
  const [currentDate, setCurrentDate] = useState(new Date());
  const [asteroids, setAsteroids] = useState<AsteroidData[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedAsteroid, setSelectedAsteroid] = useState<AsteroidData | null>(null);
  const [hazardousCount, setHazardousCount] = useState(0);

  const formattedDate = format(currentDate, 'yyyy-MM-dd');

  useEffect(() => {
    const controller = new AbortController();

    const fetchData = async () => {
      setLoading(true);
      try {
        const res = await fetch(`${API_BASE}/api/asteroids?date=${formattedDate}`, {
          signal: controller.signal,
        });
        if (!res.ok) throw new Error(`API error: ${res.status}`);
        const json = await res.json();
        const data: AsteroidData[] = json.asteroids ?? [];
        setAsteroids(data);
        setHazardousCount(data.filter((a) => a.is_potentially_hazardous).length);
      } catch (err: unknown) {
        if (err instanceof DOMException && err.name === 'AbortError') return;
        console.error('Failed to fetch asteroids', err);
      }
      setLoading(false);
    };

    fetchData();
    return () => controller.abort();
  }, [formattedDate]);

  const goBack = useCallback(() => setCurrentDate((d) => subDays(d, 1)), []);
  const goForward = useCallback(() => setCurrentDate((d) => addDays(d, 1)), []);

  return (
    <main className="main-container">
      {/* ── HUD Overlay ────────────────────────────── */}
      <div className="hud-overlay">
        <div className="hud-left">
          <h1 className="title">NEO ORBITAL TRACKER</h1>
          <p className="subtitle">Multi-Agency Comparative Analysis Engine</p>
          <div className="date-picker glass-panel">
            <button onClick={goBack} className="icon-btn" aria-label="Previous day">
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><polyline points="15 18 9 12 15 6" /></svg>
            </button>
            <div className="date-display">
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#60a5fa" strokeWidth="2"><rect x="3" y="4" width="18" height="18" rx="2" /><line x1="16" y1="2" x2="16" y2="6" /><line x1="8" y1="2" x2="8" y2="6" /><line x1="3" y1="10" x2="21" y2="10" /></svg>
              <span className="font-mono">{formattedDate}</span>
            </div>
            <button onClick={goForward} className="icon-btn" aria-label="Next day">
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><polyline points="9 18 15 12 9 6" /></svg>
            </button>
          </div>
        </div>

        <div className="hud-right">
          <div className="stats-card glass-panel">
            <h3 className="stats-label">Objects Tracked</h3>
            <p className="stats-value font-mono">{asteroids.length}</p>
          </div>
          <div className="stats-card glass-panel hazardous-card">
            <h3 className="stats-label">Hazardous</h3>
            <p className="stats-value font-mono hazardous-value">{hazardousCount}</p>
          </div>
        </div>
      </div>

      {/* ── 3D Canvas ──────────────────────────────── */}
      <div className="canvas-container">
        {loading && (
          <div className="loading-overlay">
            <div className="loading-text font-mono">CALCULATING ORBITS...</div>
          </div>
        )}
        <SolarSystem asteroids={asteroids} onSelect={setSelectedAsteroid} />
      </div>

      {/* ── Info Panel ─────────────────────────────── */}
      {selectedAsteroid && (
        <div className="info-panel glass-panel">
          <div className="info-header">
            <h2 className="info-title font-mono">{selectedAsteroid.name}</h2>
            {selectedAsteroid.is_potentially_hazardous && (
              <span className="hazard-badge">⚠ HAZARDOUS</span>
            )}
          </div>

          <div className="info-content">
            <div className="info-grid">
              <div className="info-box">
                <p className="info-box-label">Approach Date</p>
                <p className="info-box-value font-mono">{selectedAsteroid.close_approach_date}</p>
              </div>
              <div className="info-box">
                <p className="info-box-label">Miss Distance</p>
                <p className="info-box-value font-mono">
                  {selectedAsteroid.miss_distance_km?.toLocaleString(undefined, { maximumFractionDigits: 0 })} km
                </p>
              </div>
              <div className="info-box">
                <p className="info-box-label">Velocity</p>
                <p className="info-box-value font-mono">
                  {selectedAsteroid.relative_velocity_km_s?.toFixed(2)} km/s
                </p>
              </div>
              <div className="info-box">
                <p className="info-box-label">Max Diameter</p>
                <p className="info-box-value font-mono">
                  {selectedAsteroid.estimated_diameter_km_max?.toFixed(3)} km
                </p>
              </div>
            </div>

            <button className="close-btn font-mono" onClick={() => setSelectedAsteroid(null)}>
              CLOSE
            </button>
          </div>
        </div>
      )}
    </main>
  );
}
