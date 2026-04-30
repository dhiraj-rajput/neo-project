"""
JPL Fireball (Bolide) API Client

Extracts ALL 9 fields from the Fireball API — nothing stripped.
Verified live: date, energy, impact-e, lat, lat-dir, lon, lon-dir, alt, vel.

API docs: https://ssd-api.jpl.nasa.gov/doc/fireball.html
"""

from src.agencies.base import BaseClient, safe_float
from src.logger import logger

_float = safe_float


class FireballClient(BaseClient):

    def __init__(self, http_client=None, semaphore=None):
        super().__init__(
            name="JPL_Fireball",
            base_url="https://ssd-api.jpl.nasa.gov",
            rate_limit=0.3,
            http_client=http_client,
            semaphore=semaphore,
        )

    async def fetch(self, limit: int = 100) -> list[dict]:
        """
        Fetch recent fireball events.
        Returns list of flat dicts matching neo_fireball_events columns.
        """
        data = await self._get("/fireball.api", params={
            "limit": str(limit),
            "req-loc": "true",
        })

        if not data or "data" not in data:
            return []

        fields = data.get("fields", [])
        results = []

        for row in data["data"]:
            entry = dict(zip(fields, row))

            event_date = entry.get("date")
            if not event_date:
                continue

            results.append({
                "event_date": event_date,
                # NOTE: API 'energy' field is in units of 10^10 Joules (not raw J).
                # Stored as-is; Grafana labels should show "Energy (10^10 J)".
                # See: https://ssd-api.jpl.nasa.gov/doc/fireball.html
                "total_radiated_energy_j": _float(entry.get("energy")),
                "impact_energy_kt": _float(entry.get("impact-e")),
                "latitude": _float(entry.get("lat")),
                "latitude_dir": entry.get("lat-dir"),
                "longitude": _float(entry.get("lon")),
                "longitude_dir": entry.get("lon-dir"),
                "altitude_km": _float(entry.get("alt")),
                "velocity_km_s": _float(entry.get("vel")),
            })

        logger.info(f"Fireball: fetched {len(results)} events")
        return results

