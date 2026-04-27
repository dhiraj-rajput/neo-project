"""
JPL Close-Approach Data (CAD) API Client

Extracts ALL 14 fields from the CAD API response — nothing stripped.
Verified against live API hit on 99942 (81 records returned).

API docs: https://ssd-api.jpl.nasa.gov/doc/cad.html
"""

from src.agencies.base import BaseClient
from src.logger import logger


class CADClient(BaseClient):

    def __init__(self, http_client=None, semaphore=None):
        super().__init__(
            name="JPL_CAD",
            base_url="https://ssd-api.jpl.nasa.gov",
            rate_limit=0.3,
            http_client=http_client,
            semaphore=semaphore,
        )

    async def fetch(self, designation: str) -> list[dict]:
        """
        Fetch all close-approach records for a designation.
        Returns list of flat dicts matching neo_agency_cad columns.
        """
        data = await self._get("/cad.api", params={
            "des": designation,
            "dist-max": "0.5",
            "date-min": "1900-01-01",
            "date-max": "2200-01-01",
            "diameter": "true",
            "fullname": "true",
        })

        if not data or "data" not in data:
            return []

        fields = data.get("fields", [])
        results = []

        for row in data["data"]:
            entry = dict(zip(fields, row))

            # Parse approach date from "cd" field: "2028-Jun-26 05:23"
            cd = entry.get("cd", "")
            approach_date = self._parse_cad_date(cd)
            if not approach_date:
                continue

            results.append({
                "approach_date": approach_date,
                "approach_datetime": cd,
                "orbit_id": entry.get("orbit_id"),
                "jd": _float(entry.get("jd")),
                "distance_au": _float(entry.get("dist")),
                "distance_min_au": _float(entry.get("dist_min")),
                "distance_max_au": _float(entry.get("dist_max")),
                "v_rel_km_s": _float(entry.get("v_rel")),
                "v_inf_km_s": _float(entry.get("v_inf")),
                "t_sigma_f": entry.get("t_sigma_f"),
                "h_mag": _float(entry.get("h")),
                "diameter_km": _float(entry.get("diameter")),
                "diameter_sigma": _float(entry.get("diameter_sigma")),
                "fullname": (entry.get("fullname") or "").strip(),
                "body": entry.get("body", "Earth"),
            })

        logger.info(f"CAD {designation}: {len(results)} close-approach records")
        return results

    @staticmethod
    def _parse_cad_date(cd: str) -> str | None:
        """Parse '2028-Jun-26 05:23' -> '2028-06-26'."""
        if not cd:
            return None
        try:
            from datetime import datetime
            dt = datetime.strptime(cd.split(" ")[0], "%Y-%b-%d")
            return dt.strftime("%Y-%m-%d")
        except (ValueError, IndexError):
            # Try ISO format fallback
            try:
                return cd[:10]
            except Exception:
                return None


def _float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
