import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "NEO Orbital Tracker — Real-Time Asteroid Monitoring",
  description: "Multi-agency comparative analysis engine for Near-Earth Object tracking using NASA, ESA, and IAU data.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body suppressHydrationWarning>{children}</body>
    </html>
  );
}
