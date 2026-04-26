'use client';
import React, { useState, useEffect, useCallback } from 'react';
import { format, addDays, subDays } from 'date-fns';
import dynamic from 'next/dynamic';
import { motion, AnimatePresence } from 'framer-motion';
import { Calendar, ChevronLeft, ChevronRight, Orbit, AlertTriangle, Activity } from 'lucide-react';
import type { AsteroidData } from '@/components/SolarSystem';
import { HudFrame, TargetingUI } from '@/components/ui/animated-hud-targeting-ui';
import { HyperText } from '@/components/ui/hyper-text';

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
    <HudFrame>
      <main className="relative w-full h-screen overflow-hidden bg-[#010104] text-white">
        
        {/* Background 3D Canvas */}
        <div className="absolute inset-0 z-0">
          <SolarSystem asteroids={asteroids} onSelect={setSelectedAsteroid} />
        </div>

        {/* Loading Overlay */}
        <AnimatePresence>
          {loading && (
            <motion.div 
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="absolute inset-0 flex flex-col items-center justify-center bg-[#010104]/80 backdrop-blur-sm z-40 pointer-events-none"
            >
              <Orbit className="w-16 h-16 text-[#00aaff] animate-spin mb-4" />
              <HyperText text="CALCULATING TRAJECTORIES..." className="text-[#00aaff] text-2xl tracking-widest" />
            </motion.div>
          )}
        </AnimatePresence>

        {/* UI Overlay */}
        <div className="absolute inset-0 z-10 pointer-events-none p-8 flex flex-col justify-between">
          
          {/* Top Header */}
          <div className="flex justify-between items-start">
            <div className="pointer-events-auto backdrop-blur-md bg-white/5 border border-white/10 p-6 rounded-2xl shadow-2xl">
              <HyperText text="NEO ORBITAL TRACKER" className="text-2xl font-bold text-[#00aaff] tracking-widest mb-1" />
              <p className="text-xs text-[#00aaff]/60 uppercase tracking-[0.2em] font-mono">Multi-Agency Analysis Engine</p>
              
              <div className="mt-6 flex items-center space-x-4">
                <button onClick={goBack} className="p-2 hover:bg-white/10 rounded-lg transition-colors border border-white/5">
                  <ChevronLeft className="w-5 h-5 text-[#00aaff]" />
                </button>
                <div className="flex items-center space-x-3 px-4 py-2 bg-black/40 rounded-lg border border-white/10">
                  <Calendar className="w-4 h-4 text-[#00aaff]" />
                  <span className="font-mono text-[#00aaff]" suppressHydrationWarning>{formattedDate}</span>
                </div>
                <button onClick={goForward} className="p-2 hover:bg-white/10 rounded-lg transition-colors border border-white/5">
                  <ChevronRight className="w-5 h-5 text-[#00aaff]" />
                </button>
              </div>
            </div>

            {/* Top Right Stats */}
            <div className="flex space-x-4 pointer-events-auto">
              <motion.div 
                initial={{ x: 50, opacity: 0 }}
                animate={{ x: 0, opacity: 1 }}
                transition={{ delay: 0.2 }}
                className="backdrop-blur-md bg-white/5 border border-white/10 p-4 rounded-2xl flex flex-col items-end min-w-[140px]"
              >
                <div className="flex items-center space-x-2 mb-2">
                  <Activity className="w-4 h-4 text-[#00aaff]" />
                  <span className="text-xs text-[#00aaff]/80 uppercase tracking-wider font-mono">Tracked</span>
                </div>
                <span className="text-4xl font-mono font-light text-[#00aaff]">{asteroids.length}</span>
              </motion.div>

              <motion.div 
                initial={{ x: 50, opacity: 0 }}
                animate={{ x: 0, opacity: 1 }}
                transition={{ delay: 0.3 }}
                className="backdrop-blur-md bg-red-900/20 border border-red-500/30 p-4 rounded-2xl flex flex-col items-end min-w-[140px]"
              >
                <div className="flex items-center space-x-2 mb-2">
                  <AlertTriangle className="w-4 h-4 text-red-500" />
                  <span className="text-xs text-red-400 uppercase tracking-wider font-mono">Hazardous</span>
                </div>
                <span className="text-4xl font-mono font-light text-red-500">{hazardousCount}</span>
              </motion.div>
            </div>
          </div>

          {/* Central Targeting Reticle */}
          <div className="absolute inset-0 flex items-center justify-center pointer-events-none mix-blend-screen opacity-40">
            <TargetingUI pathColors={{ light: "#00aaff", dark: "#00aaff" }} className="w-[800px] h-[800px] text-[#00aaff]" />
          </div>

          {/* Bottom Selected Asteroid Panel */}
          <AnimatePresence>
            {selectedAsteroid && (
              <motion.div 
                initial={{ y: 100, opacity: 0 }}
                animate={{ y: 0, opacity: 1 }}
                exit={{ y: 100, opacity: 0 }}
                className="absolute bottom-8 left-1/2 -translate-x-1/2 pointer-events-auto w-full max-w-3xl"
              >
                <div className="backdrop-blur-xl bg-black/60 border border-[#00aaff]/30 p-6 rounded-2xl shadow-[0_0_50px_rgba(0,170,255,0.1)] relative overflow-hidden">
                  
                  {/* Decorative Scanline */}
                  <motion.div 
                    className="absolute inset-0 h-1 bg-[#00aaff]/50 w-full"
                    animate={{ top: ["0%", "100%", "0%"] }}
                    transition={{ duration: 4, repeat: Infinity, ease: "linear" }}
                  />

                  <div className="flex justify-between items-start mb-6">
                    <div>
                      <div className="flex items-center space-x-3">
                        <HyperText text={selectedAsteroid.name} className="text-3xl font-mono text-white" duration={600} />
                        {selectedAsteroid.is_potentially_hazardous && (
                          <span className="px-3 py-1 bg-red-500/20 border border-red-500/50 text-red-500 text-xs font-mono tracking-widest rounded animate-pulse">
                            HAZARD DETECTED
                          </span>
                        )}
                      </div>
                      <span className="text-xs text-white/40 font-mono mt-1 block">ID: {selectedAsteroid.asteroid_id}</span>
                    </div>
                    <button 
                      onClick={() => setSelectedAsteroid(null)}
                      className="text-xs font-mono text-[#00aaff] hover:text-white transition-colors border border-[#00aaff]/30 px-4 py-2 rounded"
                    >
                      [ CLOSE ]
                    </button>
                  </div>

                  <div className="grid grid-cols-4 gap-4">
                    <div className="bg-white/5 border border-white/10 p-4 rounded-xl">
                      <p className="text-[10px] text-[#00aaff]/60 uppercase tracking-widest mb-1">Approach Date</p>
                      <p className="font-mono text-lg">{selectedAsteroid.close_approach_date}</p>
                    </div>
                    <div className="bg-white/5 border border-white/10 p-4 rounded-xl">
                      <p className="text-[10px] text-[#00aaff]/60 uppercase tracking-widest mb-1">Miss Dist. (km)</p>
                      <p className="font-mono text-lg">{selectedAsteroid.miss_distance_km.toLocaleString(undefined, { maximumFractionDigits: 0 })}</p>
                    </div>
                    <div className="bg-white/5 border border-white/10 p-4 rounded-xl">
                      <p className="text-[10px] text-[#00aaff]/60 uppercase tracking-widest mb-1">Velocity (km/s)</p>
                      <p className="font-mono text-lg">{selectedAsteroid.relative_velocity_km_s.toFixed(2)}</p>
                    </div>
                    <div className="bg-white/5 border border-white/10 p-4 rounded-xl">
                      <p className="text-[10px] text-[#00aaff]/60 uppercase tracking-widest mb-1">Max Dia. (km)</p>
                      <p className="font-mono text-lg">{selectedAsteroid.estimated_diameter_km_max.toFixed(3)}</p>
                    </div>
                  </div>
                </div>
              </motion.div>
            )}
          </AnimatePresence>

        </div>
      </main>
    </HudFrame>
  );
}
