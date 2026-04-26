'use client';

import React, { useRef, useMemo, useCallback, useImperativeHandle } from 'react';
import { Canvas, useFrame, useThree } from '@react-three/fiber';
import { OrbitControls, Stars } from '@react-three/drei';
import { EffectComposer, Bloom, Vignette } from '@react-three/postprocessing';
import * as THREE from 'three';
import { Sun, Planets, AsteroidBelt } from './three/SolarBodies';

// ── Types ──────────────────────────────────────────────
export interface AsteroidData {
  asteroid_id: string;
  name: string;
  close_approach_date: string;
  is_potentially_hazardous: boolean;
  estimated_diameter_km_max: number;
  relative_velocity_km_s: number;
  miss_distance_km: number;
  absolute_magnitude_h?: number;
  nasa_jpl_url?: string;
}

export interface SolarSystemHandle {
  flyTo: (asteroidId: string) => void;
}

interface SolarSystemProps {
  asteroids: AsteroidData[];
  onSelect: (data: AsteroidData | null) => void;
  solarRef?: React.RefObject<SolarSystemHandle | null>;
}

// ── Camera Controller with Fly-To ──────────────────────
function CameraController({ targetPos, onArrived }: { targetPos: THREE.Vector3 | null; onArrived?: () => void }) {
  const { camera } = useThree();
  const arrivedRef = useRef(false);

  useFrame(() => {
    if (!targetPos) { arrivedRef.current = false; return; }
    if (arrivedRef.current) return;

    const offset = targetPos.clone().add(new THREE.Vector3(3, 2, 5));
    camera.position.lerp(offset, 0.02);

    if (camera.position.distanceTo(offset) < 0.5) {
      arrivedRef.current = true;
      onArrived?.();
    }
  });

  return null;
}

// ── Single NEO Asteroid ────────────────────────────────
function NEOAsteroid({
  data,
  onSelect,
  positionsRef,
}: {
  data: AsteroidData;
  onSelect: (d: AsteroidData) => void;
  positionsRef: React.MutableRefObject<Map<string, THREE.Vector3>>;
}) {
  const meshRef = useRef<THREE.Mesh>(null);

  const { radius, speed, angleOffset, inclination } = useMemo(() => {
    let hash = 0;
    for (let i = 0; i < data.asteroid_id.length; i++) {
      hash = (hash * 31 + data.asteroid_id.charCodeAt(i)) | 0;
    }
    const pr = Math.abs(hash % 1000) / 1000;
    const lunarDist = data.miss_distance_km / 384400;
    const r = Math.max(10, Math.min(55, 9 + lunarDist * 0.5));
    return {
      radius: r,
      speed: Math.max(0.003, Math.min(0.015, (data.relative_velocity_km_s / 30) * 0.008)),
      angleOffset: pr * Math.PI * 2,
      inclination: (pr - 0.5) * 0.4,
    };
  }, [data.asteroid_id, data.miss_distance_km, data.relative_velocity_km_s]);

  const color = data.is_potentially_hazardous ? '#ff2200' : '#44bbff';
  const emissive = data.is_potentially_hazardous ? '#ff0000' : '#0077dd';
  const emissiveIntensity = data.is_potentially_hazardous ? 3.0 : 1.2;
  const size = Math.max(0.15, Math.min(0.8, (data.estimated_diameter_km_max || 0.05) * 1.5));

  useFrame(({ clock }) => {
    if (!meshRef.current) return;
    const t = clock.getElapsedTime() * speed + angleOffset;
    const x = Math.cos(t) * radius;
    const y = Math.sin(t) * radius * inclination;
    const z = Math.sin(t) * radius;
    meshRef.current.position.set(x, y, z);
    meshRef.current.rotation.y += 0.01;
    positionsRef.current.set(data.asteroid_id, new THREE.Vector3(x, y, z));
  });

  const handleClick = useCallback((e: { stopPropagation: () => void }) => {
    e.stopPropagation();
    onSelect(data);
  }, [data, onSelect]);

  return (
    <mesh
      ref={meshRef}
      onClick={handleClick}
      onPointerOver={() => { document.body.style.cursor = 'crosshair'; }}
      onPointerOut={() => { document.body.style.cursor = 'default'; }}
    >
      <dodecahedronGeometry args={[size, 1]} />
      <meshStandardMaterial
        color={color}
        roughness={0.3}
        metalness={0.7}
        emissive={emissive}
        emissiveIntensity={emissiveIntensity}
        toneMapped={false}
      />
      {data.is_potentially_hazardous && (
        <mesh>
          <sphereGeometry args={[size * 1.4, 12, 12]} />
          <meshBasicMaterial color="#ff0000" transparent opacity={0.12} wireframe />
        </mesh>
      )}
    </mesh>
  );
}

// ── Scene Content ──────────────────────────────────────
function SceneContent({ asteroids, onSelect, solarRef }: SolarSystemProps) {
  const positionsRef = useRef<Map<string, THREE.Vector3>>(new Map());
  const [flyTarget, setFlyTarget] = React.useState<THREE.Vector3 | null>(null);

  useImperativeHandle(solarRef, () => ({
    flyTo: (asteroidId: string) => {
      const pos = positionsRef.current.get(asteroidId);
      if (pos) setFlyTarget(pos.clone());
    },
  }));

  return (
    <>
      <color attach="background" args={['#010104']} />
      <ambientLight intensity={0.15} />
      <Stars radius={300} depth={120} count={4000} factor={4} saturation={0} fade speed={0.5} />

      <Sun />
      <Planets />
      <AsteroidBelt />

      {asteroids.map((ast: AsteroidData) => (
        <NEOAsteroid
          key={ast.asteroid_id}
          data={ast}
          onSelect={onSelect}
          positionsRef={positionsRef}
        />
      ))}

      <CameraController targetPos={flyTarget} onArrived={() => setFlyTarget(null)} />

      <OrbitControls
        enablePan
        enableZoom
        enableRotate
        autoRotate
        autoRotateSpeed={0.3}
        maxDistance={200}
        minDistance={5}
        enableDamping
        dampingFactor={0.05}
      />

      <EffectComposer>
        <Bloom luminanceThreshold={0.4} mipmapBlur intensity={1.2} radius={0.7} />
        <Vignette eskil={false} offset={0.1} darkness={0.8} />
      </EffectComposer>
    </>
  );
}

// ── Main Canvas Wrapper ────────────────────────────────
export default function SolarSystem({ asteroids, onSelect, solarRef }: SolarSystemProps) {
  return (
    <Canvas
      camera={{ position: [0, 30, 60], fov: 50 }}
      gl={{ antialias: false, powerPreference: 'high-performance' }}
      dpr={[1, 1.5]}
    >
      <SceneContent asteroids={asteroids} onSelect={onSelect} solarRef={solarRef} />
    </Canvas>
  );
}
