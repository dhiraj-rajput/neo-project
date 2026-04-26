'use client';

import React, { useState, useEffect } from 'react';
import { X, AlertTriangle, Globe, Shield, Telescope, Crosshair } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

interface OrbitalElement {
  title: string;
  value: string;
  units: string;
}

interface PhysicalParam {
  title: string;
  value: string;
  units: string;
}

interface DiscoveryInfo {
  date: string;
  site: string;
}

interface JplSbdb {
  orbital_elements?: Record<string, OrbitalElement>;
  physical_params?: Record<string, PhysicalParam>;
  discovery?: DiscoveryInfo;
}

interface JplSentry {
  status: string;
  message?: string;
  impact_probability?: string;
  torino_scale?: string;
  palermo_scale?: string;
  diameter_km?: string;
}

interface CloseApproach {
  cd: string;
  dist: string;
  v_rel: string;
}

interface JplCad {
  approaches?: CloseApproach[];
  count?: number;
}

interface EsaNeocc {
  on_risk_list: boolean;
}

interface IauMpc {
  status: string;
}

interface AsteroidProfileData {
  name: string;
  pha: boolean;
  agencies: {
    jpl_sbdb?: JplSbdb;
    jpl_sentry?: JplSentry;
    jpl_cad?: JplCad;
    esa_neocc?: EsaNeocc;
    iau_mpc?: IauMpc;
  };
}

interface AsteroidProfileProps {
  designation: string | null;
  onClose: () => void;
}

export default function AsteroidProfile({ designation, onClose }: AsteroidProfileProps) {
  const [profile, setProfile] = useState<AsteroidProfileData | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!designation) { setProfile(null); return; }
    let cancelled = false;
    setLoading(true);
    fetch(`${API_BASE}/api/asteroid/${encodeURIComponent(designation)}`)
      .then(r => r.json())
      .then(data => { if (!cancelled) setProfile(data as AsteroidProfileData); })
      .catch(() => {})
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [designation]);

  if (!designation) return null;

  const sbdb = profile?.agencies?.jpl_sbdb;
  const sentry = profile?.agencies?.jpl_sentry;
  const cad = profile?.agencies?.jpl_cad;
  const esa = profile?.agencies?.esa_neocc;
  const mpc = profile?.agencies?.iau_mpc;

  return (
    <AnimatePresence>
      <motion.div
        key="profile"
        initial={{ x: 400, opacity: 0 }}
        animate={{ x: 0, opacity: 1 }}
        exit={{ x: 400, opacity: 0 }}
        transition={{ type: 'spring', damping: 25, stiffness: 200 }}
        className="neo-profile-panel"
      >
        <div className="neo-profile-header">
          <div>
            <h2>{profile?.name || designation}</h2>
            {profile?.pha && (
              <span className="neo-pha-badge"><AlertTriangle size={12} /> PHA</span>
            )}
          </div>
          <button onClick={onClose} className="neo-profile-close"><X size={18} /></button>
        </div>

        {loading ? (
          <div className="neo-profile-loading">
            <div className="neo-spinner" />
            <p>Querying 5 agencies…</p>
          </div>
        ) : profile ? (
          <div className="neo-profile-sections">
            {/* JPL SBDB */}
            {sbdb && (
              <section className="neo-agency-section">
                <h3><Globe size={14} /> 🇺🇸 NASA/JPL SBDB</h3>
                {sbdb.orbital_elements && (
                  <div className="neo-data-grid">
                    {Object.entries(sbdb.orbital_elements).slice(0, 8).map(([key, el]) => (
                      <div key={key} className="neo-data-item">
                        <span className="neo-data-label">{el.title || key}</span>
                        <span className="neo-data-value">{el.value} {el.units || ''}</span>
                      </div>
                    ))}
                  </div>
                )}
                {sbdb.physical_params && (
                  <div className="neo-data-grid">
                    {Object.entries(sbdb.physical_params).slice(0, 6).map(([key, p]) => (
                      <div key={key} className="neo-data-item">
                        <span className="neo-data-label">{p.title || key}</span>
                        <span className="neo-data-value">{p.value} {p.units || ''}</span>
                      </div>
                    ))}
                  </div>
                )}
                {sbdb.discovery && (
                  <div className="neo-data-grid">
                    <div className="neo-data-item">
                      <span className="neo-data-label">Discovered</span>
                      <span className="neo-data-value">{sbdb.discovery.date}</span>
                    </div>
                    <div className="neo-data-item">
                      <span className="neo-data-label">Site</span>
                      <span className="neo-data-value">{sbdb.discovery.site}</span>
                    </div>
                  </div>
                )}
              </section>
            )}

            {/* Sentry Impact Risk */}
            {sentry && (
              <section className="neo-agency-section">
                <h3><Shield size={14} /> 🇺🇸 JPL Sentry Risk</h3>
                {sentry.status === 'removed' ? (
                  <p className="neo-safe-text">✅ {sentry.message || 'Removed from risk list'}</p>
                ) : (
                  <div className="neo-data-grid">
                    <div className="neo-data-item">
                      <span className="neo-data-label">Impact Prob</span>
                      <span className="neo-data-value neo-danger">{sentry.impact_probability}</span>
                    </div>
                    <div className="neo-data-item">
                      <span className="neo-data-label">Torino Scale</span>
                      <span className="neo-data-value">{sentry.torino_scale}</span>
                    </div>
                    <div className="neo-data-item">
                      <span className="neo-data-label">Palermo Scale</span>
                      <span className="neo-data-value">{sentry.palermo_scale}</span>
                    </div>
                    <div className="neo-data-item">
                      <span className="neo-data-label">Diameter</span>
                      <span className="neo-data-value">{sentry.diameter_km} km</span>
                    </div>
                  </div>
                )}
              </section>
            )}

            {/* Close Approaches */}
            {cad && cad.approaches && cad.approaches.length > 0 && (
              <section className="neo-agency-section">
                <h3><Crosshair size={14} /> 🇺🇸 Close Approaches ({cad.count})</h3>
                <div className="neo-cad-list">
                  {cad.approaches.slice(0, 8).map((a, i) => (
                    <div key={i} className="neo-cad-row">
                      <span className="neo-cad-date">{a.cd}</span>
                      <span className="neo-cad-dist">{parseFloat(a.dist).toFixed(6)} AU</span>
                      <span className="neo-cad-vel">{parseFloat(a.v_rel).toFixed(1)} km/s</span>
                    </div>
                  ))}
                </div>
              </section>
            )}

            {/* ESA NEOCC */}
            {esa && (
              <section className="neo-agency-section">
                <h3><Globe size={14} /> 🇪🇺 ESA NEOCC</h3>
                <p className={esa.on_risk_list ? 'neo-danger-text' : 'neo-safe-text'}>
                  {esa.on_risk_list ? '⚠️ On ESA risk list' : '✅ Not on ESA risk list'}
                </p>
              </section>
            )}

            {/* MPC */}
            {mpc && (
              <section className="neo-agency-section">
                <h3><Telescope size={14} /> 🌍 IAU/MPC</h3>
                <p className="neo-mpc-status">
                  {mpc.status === 'found' ? '✅ Official IAU orbit available' : 'ℹ️ Not in MPC catalog'}
                </p>
              </section>
            )}
          </div>
        ) : null}
      </motion.div>
    </AnimatePresence>
  );
}
