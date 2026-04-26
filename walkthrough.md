# NEO Platform — Sci-Fi Modernization

The Asteroid Analytics platform has been fully upgraded with a modern, high-performance tech stack utilizing Tailwind CSS, Framer Motion, and official Three.js post-processing tools.

## What Changed?

1. **Infrastructure Upgrade**: 
   - Replaced basic CSS with **Tailwind CSS**.
   - Integrated **Framer Motion** for smooth, hardware-accelerated animations.
2. **Sci-Fi Targeting HUD**:
   - Built a sleek, glassmorphism UI overlaid on the 3D scene using components inspired by `21st.dev`.
   - Added a global `Animated HUD Targeting UI` frame with subtle SVG line animations to frame the screen like a command center.
   - Replaced static text with a `HyperText` typewriter/decoding component for the title and asteroid data reveals.
3. **Cinematic 3D Graphics**:
   - Refactored `SolarSystem.tsx` to use `@react-three/postprocessing`.
   - Added a **Bloom** effect to make hazardous asteroids physically glow like embers.
   - Upgraded the Earth from a flat blue sphere into a holographic wireframe node (`Wireframe` geometry) with atmospheric blending, perfectly matching the HUD aesthetic.
   - Enhanced lighting with deep-space stars and high-contrast point lights.

## How to View and Test

1. Ensure the platform is running:
```bash
docker-compose up -d
```
2. Make sure the Kafka producer and Spark processor are running in the background to supply data:
```bash
docker exec -it neo-app python -m src.producer.neo_producer
docker exec -it neo-app python -m src.consumer.spark_processor
```

3. **Open your browser** and navigate to:
   [http://localhost:3000](http://localhost:3000)

### What to Look For:
- **Animations**: Watch the targeting reticle load in when you refresh the page.
- **Glassmorphism UI**: Notice how the stats cards at the top blur the 3D scene behind them.
- **Interactivity**: Click on an asteroid (especially a red hazardous one). A glass panel will smoothly slide up from the bottom with typing text revealing the asteroid's critical data.
- **Graphics**: Rotate the camera to see the holographic Earth wireframe and the intense bloom effects on the asteroids against the deep starfield.

> [!TIP]
> The UI is completely responsive, but best experienced in fullscreen mode on a desktop to appreciate the intricate SVG targeting overlays.
