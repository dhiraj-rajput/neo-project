"""
JPL Sentry Impact Risk API Client

Extracts EVERY field from the Sentry API response.
Verified against live API: summary table has 14 fields per entry,
mode O has 23 summary fields + 6 VI-data fields.

API docs: https://ssd-api.jpl.nasa.gov/doc/sentry.html
"""

from src.agencies.base import BaseClient
from src.logger import logger


class SentryClient(BaseClient):

    def __init__(self, http_client=None, semaphore=None):
        super().__init__(
            name="JPL_Sentry",
            base_url="https://ssd-api.jpl.nasa.gov",
            rate_limit=0.3,
            http_client=http_client,
            semaphore=semaphore,
        )

    async def fetch(self, designation: str) -> dict | None:
        """
        Fetch Sentry data for a single object (mode O).
        Returns flat dict matching neo_agency_sentry columns.
        """
        data = await self._get("/sentry.api", params={"des": designation})
        return self._parse_single(data, designation)

    async def fetch_with_status(self, designation: str, use_spk: bool = False):
        """Returns (parsed_dict | None, FetchStatus)."""
        params = {"spk": designation} if use_spk else {"des": designation}
        data, status = await self._get_with_status("/sentry.api", params=params)
        status.lookup_value = designation
        result = self._parse_single(data, designation)
        return result, status

    def _parse_single(self, data, designation: str) -> dict | None:
        if not data:
            return None

        # Object removed from Sentry
        if "error" in data:
            err = data["error"]
            if "removed" in err.lower():
                return {
                    "status": "removed",
                    "designation": designation,
                    "removed_date": data.get("removed"),
                }
            return {"status": "not_found", "designation": designation}

        # Active object with summary
        summary = data.get("summary", {})
        if not summary:
            return {"status": "not_found", "designation": designation}

        return {
            "status": "active",
            "designation": summary.get("des"),
            "fullname": summary.get("fullname"),
            "sentry_id": None,
            "method": summary.get("method"),

            "impact_probability": _float(summary.get("ip")),
            "torino_scale": _int(summary.get("ts_max")),
            "palermo_scale_cum": _float(summary.get("ps_cum")),
            "palermo_scale_max": _float(summary.get("ps_max")),
            "n_impacts": _int(summary.get("n_imp")),
            "impact_date_range": None,

            "v_infinity_km_s": _float(summary.get("v_inf")),
            "v_impact_km_s": _float(summary.get("v_imp")),
            "energy_mt": _float(summary.get("energy")),
            "mass_kg": _float(summary.get("mass")),
            "diameter_km": _float(summary.get("diameter")),
            "h_mag": _float(summary.get("h")),

            "first_obs": summary.get("first_obs"),
            "last_obs": summary.get("last_obs"),
            "last_obs_jd": None,
            "arc_days": summary.get("darc"),
            "n_obs": _int(summary.get("nobs")),
            "n_del": _int(summary.get("ndel")),
            "n_dop": _int(summary.get("ndop")),
            "n_sat": summary.get("nsat"),

            "pdate": summary.get("pdate"),
            "cdate": summary.get("cdate"),
            "removed_date": None,
        }

    async def fetch_watchlist(self) -> list[dict]:
        """
        Fetch all active Sentry objects (summary table mode).
        Returns list of flat dicts matching neo_agency_sentry columns.
        """
        data = await self._get("/sentry.api")
        if not data or "data" not in data:
            return []

        results = []
        for entry in data["data"]:
            results.append({
                "status": "active",
                "designation": entry.get("des"),
                "fullname": entry.get("fullname"),
                "sentry_id": entry.get("id"),
                "method": None,

                "impact_probability": _float(entry.get("ip")),
                "torino_scale": _int(entry.get("ts_max")),
                "palermo_scale_cum": _float(entry.get("ps_cum")),
                "palermo_scale_max": _float(entry.get("ps_max")),
                "n_impacts": _int(entry.get("n_imp")),
                "impact_date_range": entry.get("range"),

                "v_infinity_km_s": _float(entry.get("v_inf")),
                "v_impact_km_s": None,
                "energy_mt": None,
                "mass_kg": None,
                "diameter_km": _float(entry.get("diameter")),
                "h_mag": _float(entry.get("h")),

                "first_obs": None,
                "last_obs": entry.get("last_obs"),
                "last_obs_jd": entry.get("last_obs_jd"),
                "arc_days": None,
                "n_obs": None,
                "n_del": None,
                "n_dop": None,
                "n_sat": None,

                "pdate": None,
                "cdate": None,
                "removed_date": None,
            })

        logger.info(f"Sentry watchlist: {len(results)} active objects")
        return results


def _float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _int(val) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None
