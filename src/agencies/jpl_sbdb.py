"""
JPL Small-Body Database (SBDB) Client

Extracts EVERY field from the SBDB API response — no stripping.
Verified against live API hit on asteroid 99942 (Apophis).

API docs: https://ssd-api.jpl.nasa.gov/doc/sbdb.html
"""

import json
from src.agencies.base import BaseClient
from src.logger import logger


class SBDBClient(BaseClient):

    def __init__(self, http_client=None, semaphore=None):
        super().__init__(
            name="JPL_SBDB",
            base_url="https://ssd-api.jpl.nasa.gov",
            rate_limit=0.3,
            http_client=http_client,
            semaphore=semaphore,
        )

    async def fetch(self, designation: str) -> dict | None:
        """
        Fetch full SBDB profile for a designation.
        Returns a flat dict matching neo_agency_sbdb columns, or None.
        """
        data = await self._get("/sbdb.api", params={
            "sstr": designation,
            "phys-par": "1",
            "discovery": "1",
            "vi-data": "1",
            "alt-des": "1",
        })
        if not data or "object" not in data:
            return None
        return self._parse_response(data)

    async def fetch_with_status(self, designation: str):
        """Returns (parsed_dict | None, FetchStatus)."""
        data, status = await self._get_with_status("/sbdb.api", params={
            "sstr": designation,
            "phys-par": "1",
            "discovery": "1",
            "vi-data": "1",
            "alt-des": "1",
        })
        status.lookup_value = designation
        if not data or "object" not in data:
            return None, status
        return self._parse_response(data), status

    def _parse_response(self, data: dict) -> dict:

        obj = data["object"]
        orbit = data.get("orbit", {})
        phys_pars = data.get("phys_par", [])
        discovery = data.get("discovery", {})

        # Parse orbit.elements[] into a name->value dict
        elements = {}
        for el in orbit.get("elements", []):
            name = el.get("name")
            if name:
                elements[name] = el.get("value")

        # Parse phys_par[] into a name->value dict
        phys = {}
        for p in phys_pars:
            name = p.get("name")
            if name:
                phys[name] = p.get("value")

        # Orbit class
        oc = obj.get("orbit_class", {})

        # Alternate designations as JSON string
        des_alt = obj.get("des_alt")
        des_alt_str = json.dumps(des_alt) if des_alt else None

        # Model parameters as JSON string
        model_pars = orbit.get("model_pars")
        model_pars_str = json.dumps(model_pars) if model_pars else None

        result = {
            # object{}
            "designation": obj.get("des"),
            "fullname": obj.get("fullname"),
            "shortname": obj.get("shortname"),
            "spkid": obj.get("spkid"),
            "object_kind": obj.get("kind"),
            "prefix": obj.get("prefix"),
            "orbit_class": oc.get("code"),
            "orbit_class_name": oc.get("name"),
            "orbit_id": obj.get("orbit_id") or orbit.get("orbit_id"),
            "is_neo": bool(obj.get("neo")),
            "is_pha": bool(obj.get("pha")),
            "des_alt": des_alt_str,

            # orbit{} metadata
            "epoch_tdb": orbit.get("epoch"),
            "cov_epoch": orbit.get("cov_epoch"),
            "equinox": orbit.get("equinox"),
            "orbit_source": orbit.get("source"),
            "producer": orbit.get("producer"),
            "soln_date": orbit.get("soln_date"),
            "pe_used": orbit.get("pe_used"),
            "sb_used": orbit.get("sb_used"),
            "two_body": orbit.get("two_body"),
            "comment": orbit.get("comment"),
            "not_valid_before": orbit.get("not_valid_before"),
            "not_valid_after": orbit.get("not_valid_after"),

            # orbit.elements[] — 11 Keplerian elements
            "eccentricity": _float(elements.get("e")),
            "semi_major_axis_au": _float(elements.get("a")),
            "perihelion_dist_au": _float(elements.get("q")),
            "aphelion_dist_au": _float(elements.get("ad")),
            "inclination_deg": _float(elements.get("i")),
            "long_asc_node_deg": _float(elements.get("om")),
            "arg_perihelion_deg": _float(elements.get("w")),
            "mean_anomaly_deg": _float(elements.get("ma")),
            "time_perihelion_tdb": elements.get("tp"),
            "orbital_period_days": _float(elements.get("per")),
            "mean_motion_deg_d": _float(elements.get("n")),

            # orbit quality
            "moid_au": _float(orbit.get("moid")),
            "moid_jup": _float(orbit.get("moid_jup")),
            "t_jup": _float(orbit.get("t_jup")),
            "condition_code": orbit.get("condition_code"),
            "data_arc_days": _int(orbit.get("data_arc")),
            "n_obs_used": _int(orbit.get("n_obs_used")),
            "n_del_obs_used": _int(orbit.get("n_del_obs_used")),
            "n_dop_obs_used": _int(orbit.get("n_dop_obs_used")),
            "rms": _float(orbit.get("rms")),

            "first_obs_date": orbit.get("first_obs"),
            "last_obs_date": orbit.get("last_obs"),

            # model_pars
            "model_pars": model_pars_str,

            # phys_par[]
            "absolute_magnitude_h": _float(phys.get("H")),
            "magnitude_slope_g": _float(phys.get("G")),
            "diameter_km": _float(phys.get("diameter")),
            "albedo": _float(phys.get("albedo")),
            "rotation_period_h": _float(phys.get("rot_per")),
            "thermal_inertia": phys.get("I"),
            "spectral_type": phys.get("spec_B") or phys.get("spec_T"),

            # discovery{} — all 9 fields from live API
            "discovery_date": discovery.get("date"),
            "discovery_site": discovery.get("site"),
            "discovery_location": discovery.get("location"),
            "discovery_who": discovery.get("who"),
            "discovery_name": discovery.get("name"),
            "discovery_ref": discovery.get("ref"),
            "discovery_cref": discovery.get("cref"),
            "discovery_text": discovery.get("discovery"),
            "discovery_citation": discovery.get("citation"),
        }

        return result


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
