'use client';

import React, { useRef, useMemo } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';

interface PlanetProps {
  name: string;
  radius: number;
  orbitRadius: number;
  color: string;
  speed: number;
  emissive?: string;
  hasRings?: boolean;
  ringColor?: string;
}

const PLANET_DATA: PlanetProps[] = [
  { name: 'Mercury', radius: 0.15, orbitRadius: 4, color: '#a0a0a0', speed: 4.15 },
  { name: 'Venus', radius: 0.35, orbitRadius: 6.5, color: '#e8cda0', speed: 1.62 },
  { name: 'Earth', radius: 0.4, orbitRadius: 9, color: '#4488ff', speed: 1.0, emissive: '#002255' },
  { name: 'Mars', radius: 0.25, orbitRadius: 12, color: '#cc4422', speed: 0.53 },
  { name: 'Jupiter', radius: 1.2, orbitRadius: 22, color: '#d4a06a', speed: 0.084 },
  { name: 'Saturn', radius: 1.0, orbitRadius: 32, color: '#e8d5a0', speed: 0.034, hasRings: true, ringColor: '#c8b080' },
  { name: 'Uranus', radius: 0.7, orbitRadius: 42, color: '#88ccdd', speed: 0.012 },
  { name: 'Neptune', radius: 0.65, orbitRadius: 52, color: '#4466ff', speed: 0.006 },
];

function OrbitRing({ radius }: { radius: number }) {
  const lineObj = useMemo(() => {
    const pts: THREE.Vector3[] = [];
    for (let i = 0; i <= 128; i++) {
      const a = (i / 128) * Math.PI * 2;
      pts.push(new THREE.Vector3(Math.cos(a) * radius, 0, Math.sin(a) * radius));
    }
    const geom = new THREE.BufferGeometry().setFromPoints(pts);
    const mat = new THREE.LineBasicMaterial({ color: '#ffffff', transparent: true, opacity: 0.06 });
    return new THREE.LineLoop(geom, mat);
  }, [radius]);

  return <primitive object={lineObj} />;
}

function Planet({ name, radius, orbitRadius, color, speed, emissive, hasRings, ringColor }: PlanetProps) {
  const ref = useRef<THREE.Group>(null);

  useFrame(({ clock }) => {
    if (ref.current) {
      const t = clock.getElapsedTime() * speed * 0.1;
      ref.current.position.x = Math.cos(t) * orbitRadius;
      ref.current.position.z = Math.sin(t) * orbitRadius;
    }
  });

  return (
    <>
      <OrbitRing radius={orbitRadius} />
      <group ref={ref}>
        <mesh>
          <sphereGeometry args={[radius, 32, 32]} />
          <meshStandardMaterial
            color={color}
            emissive={emissive || color}
            emissiveIntensity={0.15}
            roughness={0.7}
            metalness={0.3}
          />
        </mesh>
        {hasRings && (
          <mesh rotation={[Math.PI / 2.5, 0, 0]}>
            <ringGeometry args={[radius * 1.4, radius * 2.2, 64]} />
            <meshBasicMaterial
              color={ringColor || color}
              transparent
              opacity={0.4}
              side={THREE.DoubleSide}
            />
          </mesh>
        )}
      </group>
    </>
  );
}

export function Planets() {
  return (
    <group>
      {PLANET_DATA.map((p) => (
        <Planet key={p.name} {...p} />
      ))}
    </group>
  );
}

export function Sun() {
  const glowRef = useRef<THREE.Mesh>(null);

  useFrame(({ clock }) => {
    if (glowRef.current) {
      const s = 1 + Math.sin(clock.getElapsedTime() * 0.5) * 0.05;
      glowRef.current.scale.setScalar(s);
    }
  });

  return (
    <group>
      <pointLight position={[0, 0, 0]} intensity={3} color="#ffdd88" distance={200} decay={1.5} />
      <mesh>
        <sphereGeometry args={[2, 48, 48]} />
        <meshBasicMaterial color="#ffcc44" toneMapped={false} />
      </mesh>
      <mesh ref={glowRef}>
        <sphereGeometry args={[2.8, 32, 32]} />
        <meshBasicMaterial
          color="#ffaa00"
          transparent
          opacity={0.15}
          blending={THREE.AdditiveBlending}
          side={THREE.BackSide}
        />
      </mesh>
      <mesh>
        <sphereGeometry args={[3.5, 24, 24]} />
        <meshBasicMaterial
          color="#ff8800"
          transparent
          opacity={0.05}
          blending={THREE.AdditiveBlending}
          side={THREE.BackSide}
        />
      </mesh>
    </group>
  );
}

// Instanced asteroid belt between Mars and Jupiter
const BELT_COUNT = 400;

export function AsteroidBelt() {
  const meshRef = useRef<THREE.InstancedMesh>(null);

  const { matrices, scales } = useMemo(() => {
    const m: THREE.Matrix4[] = [];
    const s: number[] = [];
    const dummy = new THREE.Object3D();

    for (let i = 0; i < BELT_COUNT; i++) {
      const angle = (i / BELT_COUNT) * Math.PI * 2 + (Math.random() - 0.5) * 0.3;
      const r = 15 + Math.random() * 5; // between Mars (12) and Jupiter (22)
      const y = (Math.random() - 0.5) * 1.5;
      dummy.position.set(Math.cos(angle) * r, y, Math.sin(angle) * r);
      const sc = 0.02 + Math.random() * 0.06;
      dummy.scale.setScalar(sc);
      dummy.rotation.set(Math.random() * Math.PI, Math.random() * Math.PI, 0);
      dummy.updateMatrix();
      m.push(dummy.matrix.clone());
      s.push(sc);
    }
    return { matrices: m, scales: s };
  }, []);

  useFrame(() => {
    if (!meshRef.current) return;
    const inst = meshRef.current;
    const dummy = new THREE.Object3D();
    const time = performance.now() * 0.00002;

    for (let i = 0; i < BELT_COUNT; i++) {
      matrices[i].decompose(dummy.position, dummy.quaternion, dummy.scale);
      const origAngle = Math.atan2(dummy.position.z, dummy.position.x);
      const r = Math.sqrt(dummy.position.x ** 2 + dummy.position.z ** 2);
      const newAngle = origAngle + time * (20 / r); // closer = faster
      dummy.position.x = Math.cos(newAngle) * r;
      dummy.position.z = Math.sin(newAngle) * r;
      dummy.updateMatrix();
      inst.setMatrixAt(i, dummy.matrix);
      matrices[i].copy(dummy.matrix);
    }
    inst.instanceMatrix.needsUpdate = true;
  });

  return (
    <instancedMesh ref={meshRef} args={[undefined, undefined, BELT_COUNT]}>
      <dodecahedronGeometry args={[1, 0]} />
      <meshStandardMaterial color="#888888" roughness={0.8} metalness={0.2} emissive="#333333" emissiveIntensity={0.3} />
    </instancedMesh>
  );
}

export { PLANET_DATA };
