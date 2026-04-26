'use client';
import React, { useRef, useMemo, useCallback } from 'react';
import { Canvas, useFrame, extend } from '@react-three/fiber';
import { OrbitControls, Stars, Text, Line } from '@react-three/drei';
import * as THREE from 'three';

// ── Types ──────────────────────────────────────────────
export interface AsteroidData {
  asteroid_id: string;
  name: string;
  close_approach_date: string;
  is_potentially_hazardous: boolean;
  estimated_diameter_km_max: number;
  relative_velocity_km_s: number;
  miss_distance_km: number;
}

interface AsteroidProps {
  data: AsteroidData;
  onSelect: (data: AsteroidData) => void;
}

interface SolarSystemProps {
  asteroids: AsteroidData[];
  onSelect: (data: AsteroidData | null) => void;
}

// ── Earth Component ────────────────────────────────────
function Earth() {
  const meshRef = useRef<THREE.Mesh>(null);

  useFrame(() => {
    if (meshRef.current) {
      meshRef.current.rotation.y += 0.003;
    }
  });

  return (
    <group>
      {/* Earth sphere */}
      <mesh ref={meshRef}>
        <sphereGeometry args={[2, 64, 64]} />
        <meshStandardMaterial
          color="#1a4d8f"
          roughness={0.6}
          metalness={0.15}
          emissive="#0a1e3d"
          emissiveIntensity={0.15}
        />
      </mesh>

      {/* Atmosphere glow ring */}
      <mesh>
        <sphereGeometry args={[2.15, 32, 32]} />
        <meshBasicMaterial color="#4a9eff" transparent opacity={0.08} side={THREE.BackSide} />
      </mesh>

      <Text
        position={[0, 3.5, 0]}
        fontSize={0.8}
        color="#7eb8ff"
        anchorX="center"
        anchorY="middle"
        font="https://fonts.gstatic.com/s/jetbrainsmono/v20/tDbY2o-flEEny0FZhsfKu5WU4zr3E_BX0PnT8RD8yKxjPVmUsaaDhw.woff2"
      >
        EARTH
      </Text>
    </group>
  );
}

// ── Single Asteroid ────────────────────────────────────
function Asteroid({ data, onSelect }: AsteroidProps) {
  const meshRef = useRef<THREE.Mesh>(null);

  // Derive orbit params from real data — stable across renders
  const { radius, speed, angleOffset, inclination } = useMemo(() => {
    // Seed a stable pseudo-random from asteroid_id
    let hash = 0;
    for (let i = 0; i < data.asteroid_id.length; i++) {
      hash = (hash * 31 + data.asteroid_id.charCodeAt(i)) | 0;
    }
    const pseudoRandom = Math.abs(hash % 1000) / 1000;

    const lunarDist = data.miss_distance_km / 384400;
    // Clamp orbit radius so everything is visible but spaced out
    const r = Math.max(6, Math.min(80, lunarDist * 0.8));
    return {
      radius: r,
      speed: Math.max(0.002, Math.min(0.02, (data.relative_velocity_km_s / 30) * 0.01)),
      angleOffset: pseudoRandom * Math.PI * 2,
      inclination: (pseudoRandom - 0.5) * 0.6, // slight tilt for 3D feel
    };
  }, [data.asteroid_id, data.miss_distance_km, data.relative_velocity_km_s]);

  const color = data.is_potentially_hazardous ? '#ff3333' : '#888888';
  const emissiveColor = data.is_potentially_hazardous ? '#660000' : '#222222';
  const size = Math.max(0.3, Math.min(1.5, (data.estimated_diameter_km_max || 0.1) * 3));

  // Orbit ring points (drei Line instead of native <line>)
  const ringPoints = useMemo(() => {
    const pts: [number, number, number][] = [];
    for (let i = 0; i <= 96; i++) {
      const a = (i / 96) * Math.PI * 2;
      pts.push([
        Math.cos(a) * radius,
        Math.sin(a) * radius * inclination,
        Math.sin(a) * radius,
      ]);
    }
    return pts;
  }, [radius, inclination]);

  useFrame(({ clock }) => {
    if (meshRef.current) {
      const t = clock.getElapsedTime() * speed + angleOffset;
      meshRef.current.position.x = Math.cos(t) * radius;
      meshRef.current.position.y = Math.sin(t) * radius * inclination;
      meshRef.current.position.z = Math.sin(t) * radius;
      meshRef.current.rotation.y += 0.01;
      meshRef.current.rotation.x += 0.015;
    }
  });

  const handleClick = useCallback((e: { stopPropagation: () => void }) => {
    e.stopPropagation();
    onSelect(data);
  }, [data, onSelect]);

  return (
    <group>
      {/* Orbit trail — using drei <Line> to avoid SVG <line> conflict */}
      <Line
        points={ringPoints}
        color={color}
        lineWidth={0.5}
        transparent
        opacity={0.15}
      />

      {/* Asteroid body */}
      <mesh
        ref={meshRef}
        onClick={handleClick}
        onPointerOver={() => { document.body.style.cursor = 'pointer'; }}
        onPointerOut={() => { document.body.style.cursor = 'default'; }}
      >
        <dodecahedronGeometry args={[size, 0]} />
        <meshStandardMaterial
          color={color}
          roughness={0.85}
          emissive={emissiveColor}
          emissiveIntensity={0.3}
        />
      </mesh>
    </group>
  );
}

// ── Main Scene ─────────────────────────────────────────
export default function SolarSystem({ asteroids, onSelect }: SolarSystemProps) {
  return (
    <Canvas
      camera={{ position: [0, 40, 80], fov: 50 }}
      gl={{ antialias: true, powerPreference: 'high-performance' }}
      dpr={[1, 1.5]} // Cap pixel ratio for performance
    >
      <color attach="background" args={['#020208']} />

      {/* Lighting */}
      <ambientLight intensity={0.15} />
      <pointLight position={[-200, 0, 0]} intensity={1.5} color="#fff5e0" />
      <directionalLight position={[10, 20, 10]} intensity={0.6} />

      {/* Star field */}
      <Stars radius={200} depth={80} count={3000} factor={3} saturation={0} fade speed={0.5} />

      <Earth />

      {asteroids.map((ast) => (
        <Asteroid key={ast.asteroid_id} data={ast} onSelect={onSelect} />
      ))}

      <OrbitControls
        enablePan={true}
        enableZoom={true}
        enableRotate={true}
        autoRotate={true}
        autoRotateSpeed={0.3}
        maxDistance={180}
        minDistance={8}
        enableDamping={true}
        dampingFactor={0.05}
      />
    </Canvas>
  );
}
