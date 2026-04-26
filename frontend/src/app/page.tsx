'use client';
import React, { useState, useEffect, useCallback, useRef } from 'react';
import { format, addDays, subDays } from 'date-fns';
import dynamic from 'next/dynamic';
import { motion, AnimatePresence } from 'framer-motion';
import { Calendar, ChevronLeft, ChevronRight, Orbit, AlertTriangle, Activity } from 'lucide-react';
import type { AsteroidData, SolarSystemHandle } from '@/components/SolarSystem';
import { HudFrame, TargetingUI } from '@/components/ui/animated-hud-targeting-ui';
import { HyperText } from '@/components/ui/hyper-text';
import AsteroidSearch from '@/components/AsteroidSearch';
import AsteroidProfile from '@/components/AsteroidProfile';
import SentryWatchlist from '@/components/SentryWatchlist';

const SolarSystem = dynamic(() => import('@/components/SolarSystem'), {
  ssr: false,
  loading: () => (
    <div className="absolute inset-0 flex items-center justify-center bg-[#010104] z-50">
      <HyperText text="INITIALIZING 3D ENGINE..." className="text-[#00aaff] text-xl" />
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
  const [profileDesignation, setProfileDesignation] = useState<string | null>(null);
  const solarRef = useRef<SolarSystemHandle>(null);

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

  const handleSearch = useCallback((designation: string) => {
    setProfileDesignation(designation);
    solarRef.current?.flyTo(designation);
  }, []);

  const handleSelectAsteroid = useCallback((data: AsteroidData | null) => {
    setSelectedAsteroid(data);
    if (data) {
      setProfileDesignation(data.asteroid_id);
      solarRef.current?.flyTo(data.asteroid_id);
    }
  }, []);

  return (
    <HudFrame>
      <main className="relative min-h-screen w-full overflow-hidden bg-[#010104] text-white">
        <div className="absolute inset-0 z-0">
          <SolarSystem solarRef={solarRef} asteroids={asteroids} onSelect={handleSelectAsteroid} />
          <div
            className="absolute inset-0 pointer-events-none"
            style={{
              backgroundImage:
                'radial-gradient(circle at top, rgba(0,170,255,0.18), transparent 22%), radial-gradient(circle at bottom right, rgba(0,170,255,0.08), transparent 25%)',
            }}
          />
        </div>
        <AnimatePresence>
          {loading && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="absolute inset-0 z-40 flex items-center justify-center bg-[#010104]/85 backdrop-blur-sm"
            >
              <Orbit className="w-16 h-16 text-[#00aaff] animate-spin mb-4" />
              <HyperText text="CALCULATING TRAJECTORIES..." className="text-[#00aaff] text-2xl tracking-widest" />
            </motion.div>
          )}
        </AnimatePresence>

        <div className="absolute inset-0 z-10 pointer-events-none p-6 lg:p-8 flex flex-col justify-between gap-6">
          {/* Top Bar */}
          <div className="flex flex-col lg:flex-row justify-between items-start gap-5">
            {/* Title + Date */}
            <div className="pointer-events-auto backdrop-blur-2xl bg-white/5 border border-white/10 p-6 rounded-4xl shadow-[0_30px_80px_rgba(0,0,0,0.25)] max-w-3xl">
              <HyperText text="NEO ORBITAL TRACKER" className="text-2xl font-bold text-[#00aaff] tracking-widest mb-2" />
              <p className="text-xs text-[#00aaff]/70 uppercase tracking-[0.35em] font-mono mb-4">Multi-Agency Analysis Engine · NASA · ESA · IAU</p>

              <div className="flex flex-wrap gap-2 mb-5">
                <span className="rounded-full border border-[#00aaff]/20 bg-[#00aaff]/10 px-3 py-1 text-[10px] uppercase tracking-[0.35em] text-[#9aedff]">
                  LIVE DATA FEED
                </span>
                <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-[10px] uppercase tracking-[0.35em] text-[#cbd5e1]">
                  ORBITAL TRACKING
                </span>
                <span className="rounded-full border border-white/10 bg-black/40 px-3 py-1 text-[10px] uppercase tracking-[0.35em] text-[#a5f3ff]">
                  {asteroids.length} objects loaded
                </span>
              </div>

              <div className="flex flex-col sm:flex-row items-center gap-3 sm:gap-4">
                <button onClick={goBack} className="pointer-events-auto inline-flex items-center justify-center rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-[#00aaff] transition hover:border-[#00aaff]/30 hover:bg-white/10">
                  <ChevronLeft className="w-5 h-5" />
                  <span className="ml-2 text-xs uppercase tracking-[0.3em]">prev</span>
                </button>
                <div className="inline-flex items-center gap-2 rounded-2xl border border-white/10 bg-black/40 px-4 py-3">
                  <Calendar className="w-4 h-4 text-[#00aaff]" />
                  <span className="font-mono text-[#00aaff]" suppressHydrationWarning>{formattedDate}</span>
                </div>
                <button onClick={goForward} className="pointer-events-auto inline-flex items-center justify-center rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-[#00aaff] transition hover:border-[#00aaff]/30 hover:bg-white/10">
                  <span className="mr-2 text-xs uppercase tracking-[0.3em]">next</span>
                  <ChevronRight className="w-5 h-5" />
                </button>
              </div>
            </div>

            {/* Right Side: Counters + Sentry */}
            <div className="pointer-events-auto flex flex-col gap-4 w-full lg:w-auto">
              <div className="flex flex-wrap gap-4">
                <motion.div initial={{ x: 50, opacity: 0 }} animate={{ x: 0, opacity: 1 }} transition={{ delay: 0.2 }} className="backdrop-blur-xl bg-white/5 border border-white/10 p-5 rounded-3xl shadow-lg min-w-40">
                  <div className="flex items-center gap-2 mb-2 text-[#00aaff]/80">
                    <Activity className="w-4 h-4" />
                    <span className="text-[10px] uppercase tracking-[0.35em] font-mono">Tracked</span>
                  </div>
                  <p className="text-4xl font-mono font-light text-[#00aaff]">{asteroids.length}</p>
                </motion.div>

                <motion.div initial={{ x: 50, opacity: 0 }} animate={{ x: 0, opacity: 1 }} transition={{ delay: 0.3 }} className="backdrop-blur-xl bg-[#1a0508]/80 border border-red-500/20 p-5 rounded-3xl shadow-lg min-w-40">
                  <div className="flex items-center gap-2 mb-2 text-red-300">
                    <AlertTriangle className="w-4 h-4" />
                    <span className="text-[10px] uppercase tracking-[0.35em] font-mono">Hazardous</span>
                  </div>
                  <p className="text-4xl font-mono font-light text-red-400">{hazardousCount}</p>
                </motion.div>
              </div>

              <SentryWatchlist onSelectAsteroid={handleSearch} />
            </div>
          </div>

          {/* Center: Search */}
          <div className="absolute top-6 left-1/2 -translate-x-1/2 pointer-events-auto z-20">
            <AsteroidSearch onSelect={handleSearch} />
          </div>

          {/* Center HUD overlay */}
          <div className="absolute inset-0 flex items-center justify-center pointer-events-none mix-blend-screen opacity-30">
            <TargetingUI
              pathColors={{ light: '#00aaff', dark: '#00aaff' }}
              className="text-[#00aaff]"
              style={{ width: 720, height: 720 }}
            />
          </div>

          {/* Bottom: Selected asteroid info */}
          <div className="relative pointer-events-auto w-full max-w-5xl mx-auto">
            <AnimatePresence>
              {selectedAsteroid ? (
                <motion.div
                  initial={{ y: 60, opacity: 0 }}
                  animate={{ y: 0, opacity: 1 }}
                  exit={{ y: 60, opacity: 0 }}
                  className="backdrop-blur-2xl bg-black/70 border border-[#00aaff]/25 p-6 rounded-4xl shadow-[0_0_80px_rgba(0,170,255,0.12)]"
                >
                  <div className="flex flex-col lg:flex-row justify-between gap-5 mb-6">
                    <div>
                      <div className="flex flex-wrap items-center gap-3 mb-2">
                        <HyperText text={selectedAsteroid.name} className="text-3xl font-semibold text-white" duration={500} />
                        {selectedAsteroid.is_potentially_hazardous && (
                          <span className="rounded-full border border-red-500/50 bg-red-500/15 px-3 py-1 text-xs uppercase tracking-[0.35em] text-red-300 font-semibold">
                            HAZARD DETECTED
                          </span>
                        )}
                      </div>
                      <p className="text-xs text-white/40 font-mono">ID: {selectedAsteroid.asteroid_id}</p>
                    </div>
                    <button onClick={() => setSelectedAsteroid(null)} className="rounded-2xl border border-[#00aaff]/30 bg-white/5 px-4 py-3 text-xs uppercase tracking-[0.3em] text-[#00aaff] transition hover:bg-[#00aaff]/10">
                      CLOSE
                    </button>
                  </div>
                  <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
                    <div className="rounded-3xl border border-white/10 bg-white/5 p-5">
                      <p className="text-[10px] uppercase tracking-[0.35em] text-[#00aaff]/70 mb-2">Approach Date</p>
                      <p className="text-lg font-mono">{selectedAsteroid.close_approach_date}</p>
                    </div>
                    <div className="rounded-3xl border border-white/10 bg-white/5 p-5">
                      <p className="text-[10px] uppercase tracking-[0.35em] text-[#00aaff]/70 mb-2">Miss Distance</p>
                      <p className="text-lg font-mono">{selectedAsteroid.miss_distance_km.toLocaleString(undefined, { maximumFractionDigits: 0 })} km</p>
                    </div>
                    <div className="rounded-3xl border border-white/10 bg-white/5 p-5">
                      <p className="text-[10px] uppercase tracking-[0.35em] text-[#00aaff]/70 mb-2">Velocity</p>
                      <p className="text-lg font-mono">{selectedAsteroid.relative_velocity_km_s.toFixed(2)} km/s</p>
                    </div>
                    <div className="rounded-3xl border border-white/10 bg-white/5 p-5">
                      <p className="text-[10px] uppercase tracking-[0.35em] text-[#00aaff]/70 mb-2">Estimated Size</p>
                      <p className="text-lg font-mono">{selectedAsteroid.estimated_diameter_km_max.toFixed(3)} km</p>
                    </div>
                  </div>
                </motion.div>
              ) : (
                <motion.div
                  initial={{ y: 60, opacity: 0 }}
                  animate={{ y: 0, opacity: 1 }}
                  className="backdrop-blur-xl bg-black/55 border border-white/10 p-6 rounded-4xl shadow-2xl"
                >
                  <div className="flex flex-col gap-4 sm:flex-row sm:items-center justify-between">
                    <div>
                      <h2 className="text-xl font-semibold tracking-tight text-white">NEO Watch Dashboard</h2>
                      <p className="mt-2 max-w-2xl text-sm text-[#cbd5e1]">
                        Press <kbd className="neo-kbd">/</kbd> to search asteroids across NASA, ESA, and IAU databases. Click any object in the 3D view to inspect it.
                      </p>
                    </div>
                    <div className="rounded-3xl border border-white/10 bg-white/5 px-5 py-4 text-sm text-[#a5f3ff]">
                      {loading ? 'Fetching latest feed...' : `${asteroids.length} tracked objects`}
                    </div>
                  </div>
                </motion.div>
              )}
            </AnimatePresence>
          </div>
        </div>

        {/* Multi-agency profile panel (right slide-in) */}
        <div className="absolute top-0 right-0 z-30 h-full pointer-events-auto">
          <AsteroidProfile
            designation={profileDesignation}
            onClose={() => setProfileDesignation(null)}
          />
        </div>
      </main>
    </HudFrame>
  );
}
