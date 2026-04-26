'use client';

import React, { useRef, useMemo, useCallback } from 'react';
import { Canvas, useFrame } from '@react-three/fiber';
import { OrbitControls, Stars, Text, Line, Sphere, Wireframe } from '@react-three/drei';
import { EffectComposer, Bloom, Vignette } from '@react-three/postprocessing';
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

// ── Holographic Earth ────────────────────────────────────
function Earth() {
  const meshRef = useRef<THREE.Mesh>(null);

  useFrame(() => {
    if (meshRef.current) {
      meshRef.current.rotation.y += 0.002;
    }
  });

  return (
    <group>
      {/* Earth Core */}
      <Sphere ref={meshRef} args={[2, 64, 64]}>
        <meshStandardMaterial
          color="#002244"
          emissive="#001122"
          roughness={0.8}
          metalness={0.8}
          transparent
          opacity={0.9}
        />
        {/* Holographic Wireframe Grid */}
        <Wireframe 
          simplify={true}
          thickness={0.015}
          stroke={"#00aaff"}
          dash={true}
          dashRepeats={8}
          dashLength={0.2}
        />
      </Sphere>

      {/* Atmospheric Glow Shell */}
      <Sphere args={[2.2, 32, 32]}>
        <meshBasicMaterial 
          color="#00aaff" 
          transparent 
          opacity={0.05} 
          side={THREE.BackSide} 
          blending={THREE.AdditiveBlending}
        />
      </Sphere>
      
      <Sphere args={[2.05, 32, 32]}>
        <meshBasicMaterial 
          color="#0055ff" 
          transparent 
          opacity={0.15} 
          blending={THREE.AdditiveBlending}
        />
      </Sphere>

      <Text
        position={[0, 3.8, 0]}
        fontSize={0.6}
        color="#00ccff"
        anchorX="center"
        anchorY="middle"
        font="https://fonts.gstatic.com/s/jetbrainsmono/v20/tDbY2o-flEEny0FZhsfKu5WU4zr3E_BX0PnT8RD8yKxjPVmUsaaDhw.woff2"
      >
        EARTH [0,0,0]
      </Text>
    </group>
  );
}

// ── Single Asteroid ────────────────────────────────────
function Asteroid({ data, onSelect }: AsteroidProps) {
  const meshRef = useRef<THREE.Mesh>(null);

  // Derive orbit params from real data — stable across renders
  const { radius, speed, angleOffset, inclination } = useMemo(() => {
    let hash = 0;
    for (let i = 0; i < data.asteroid_id.length; i++) {
      hash = (hash * 31 + data.asteroid_id.charCodeAt(i)) | 0;
    }
    const pseudoRandom = Math.abs(hash % 1000) / 1000;

    const lunarDist = data.miss_distance_km / 384400;
    const r = Math.max(6, Math.min(80, lunarDist * 0.8));
    return {
      radius: r,
      speed: Math.max(0.002, Math.min(0.02, (data.relative_velocity_km_s / 30) * 0.01)),
      angleOffset: pseudoRandom * Math.PI * 2,
      inclination: (pseudoRandom - 0.5) * 0.6,
    };
  }, [data.asteroid_id, data.miss_distance_km, data.relative_velocity_km_s]);

  // Use intense emissive colors for the Bloom effect
  const color = data.is_potentially_hazardous ? '#ff1100' : '#44bbff';
  const emissiveColor = data.is_potentially_hazardous ? '#ff0000' : '#0088ff';
  const emissiveIntensity = data.is_potentially_hazardous ? 4.0 : 1.5;
  const size = Math.max(0.2, Math.min(1.2, (data.estimated_diameter_km_max || 0.1) * 2));

  // Orbit ring points
  const ringPoints = useMemo(() => {
    const pts: [number, number, number][] = [];
    for (let i = 0; i <= 128; i++) {
      const a = (i / 128) * Math.PI * 2;
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
      {/* Subtle glowing orbit ring */}
      <Line
        points={ringPoints}
        color={color}
        lineWidth={0.5}
        transparent
        opacity={data.is_potentially_hazardous ? 0.3 : 0.1}
      />

      {/* Asteroid body */}
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
          metalness={0.8}
          emissive={emissiveColor}
          emissiveIntensity={emissiveIntensity}
          toneMapped={false} // Ensure bloom catches the emissive intensity
        />
        {/* Highlight ring for hazardous objects */}
        {data.is_potentially_hazardous && (
          <mesh>
            <sphereGeometry args={[size * 1.3, 16, 16]} />
            <meshBasicMaterial color="#ff0000" transparent opacity={0.2} wireframe />
          </mesh>
        )}
      </mesh>
    </group>
  );
}

// ── Main Scene ─────────────────────────────────────────
export default function SolarSystem({ asteroids, onSelect }: SolarSystemProps) {
  return (
    <Canvas
      camera={{ position: [0, 40, 80], fov: 50 }}
      gl={{ antialias: false, powerPreference: 'high-performance' }} // Anti-alias false recommended when using post-processing
      dpr={[1, 1.5]}
    >
      <color attach="background" args={['#010104']} />

      {/* Lighting */}
      <ambientLight intensity={0.2} />
      <directionalLight position={[10, 20, 10]} intensity={1.0} color="#00aaff" />
      <pointLight position={[0, 0, 0]} intensity={2.0} color="#0055ff" distance={50} />

      {/* Deep Space Stars */}
      <Stars radius={200} depth={100} count={5000} factor={4} saturation={0} fade speed={1} />

      <Earth />

      {asteroids.map((ast) => (
        <Asteroid key={ast.asteroid_id} data={ast} onSelect={onSelect} />
      ))}

      <OrbitControls
        enablePan={true}
        enableZoom={true}
        enableRotate={true}
        autoRotate={true}
        autoRotateSpeed={0.5}
        maxDistance={250}
        minDistance={8}
        enableDamping={true}
        dampingFactor={0.05}
      />

      {/* Sci-Fi Post Processing */}
      <EffectComposer>
        <Bloom 
          luminanceThreshold={0.5} 
          mipmapBlur 
          intensity={1.5} 
          radius={0.8}
        />
        <Vignette eskil={false} offset={0.1} darkness={0.9} />
      </EffectComposer>
    </Canvas>
  );
}
