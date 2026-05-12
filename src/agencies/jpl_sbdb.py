"""
JPL Small-Body Database (SBDB) Client

Two fetch modes:
  1. fetch() / fetch_with_status() — per-asteroid via sbdb.api (full profile)
  2. fetch_bulk_orbital() — bulk via sbdb_query.api (orbital/physical fields for ALL NEOs)

The bulk mode uses sbdb_query.api to fetch key orbital & physical fields
for all NEOs in a single request, eliminating ~20K per-asteroid calls.
Per-asteroid fetch is still available for fields not in sbdb_query
(discovery info, model params, etc.).

API docs:
  sbdb.api:       https://ssd-api.jpl.nasa.gov/doc/sbdb.html
  sbdb_query.api: https://ssd-api.jpl.nasa.gov/doc/sbdb_query.html
"""

import json
from src.agencies.base import BaseClient, safe_float, safe_int
from src.logger import logger

_float = safe_float
_int = safe_int

# Fields to fetch in bulk query — covers most of what we store in neo_agency_sbdb
SBDB_BULK_FIELDS = ",".join([
    "spkid", "full_name", "pdes", "name", "kind", "neo", "pha",
    "orbit_id", "class",
    "epoch", "e", "a", "q", "ad", "i", "om", "w", "ma", "tp", "per", "n",
    "moid", "moid_jup", "t_jup", "condition_code", "data_arc",
    "n_obs_used", "n_del_obs_used", "n_dop_obs_used", "rms",
    "first_obs", "last_obs",
    "H", "G", "diameter", "albedo", "rot_per",
])


class SBDBClient(BaseClient):

    def __init__(self, http_client=None, semaphore=None, rate_limiter=None):
        super().__init__(
            name="JPL_SBDB",
            base_url="https://ssd-api.jpl.nasa.gov",
            rate_limit=0.1,
            http_client=http_client,
            semaphore=semaphore,
            rate_limiter=rate_limiter,
        )

    # ── Bulk Fetch (sbdb_query.api) ───────────────────────────

    async def fetch_bulk_orbital(self) -> dict[str, dict]:
        """
        Fetch orbital & physical data for ALL NEOs in ONE API call
        using sbdb_query.api.

        Returns dict keyed by primary designation: {pdes: parsed_dict}.
        """
        logger.info("[SBDB] Fetching bulk orbital data for all NEOs via sbdb_query.api...")

        data = await self._get("/sbdb_query.api", params={
            "fields": SBDB_BULK_FIELDS,
            "sb-kind": "a",        # asteroids only
            "sb-ns": "n",          # numbered only (for speed — unnumbered fetched per-asteroid)
            "sb-group": "neo",     # NEOs only
        })

        if not data or "data" not in data:
            logger.warning("[SBDB] Bulk query returned no data")
            return {}

        fields = data.get("fields", [])
        results: dict[str, dict] = {}

        for row in data["data"]:
            entry = dict(zip(fields, row))
            pdes = entry.get("pdes", "")
            if not pdes:
                continue

            # Parse orbit class
            oc = entry.get("class", "")

            parsed = {
                "designation": pdes,
                "fullname": entry.get("full_name"),
                "shortname": None,
                "spkid": entry.get("spkid"),
                "object_kind": entry.get("kind"),
                "prefix": None,
                "orbit_class": oc,
                "orbit_class_name": None,  # Not available in query API
                "orbit_id": entry.get("orbit_id"),
                "is_neo": entry.get("neo") in ("Y", True),
                "is_pha": entry.get("pha") in ("Y", True),
                "des_alt": None,

                # Orbital elements
                "epoch_tdb": entry.get("epoch"),
                "cov_epoch": None,
                "equinox": "J2000",
                "orbit_source": None,
                "producer": None,
                "soln_date": None,
                "pe_used": None,
                "sb_used": None,
                "two_body": None,
                "comment": None,
                "not_valid_before": None,
                "not_valid_after": None,

                "eccentricity": _float(entry.get("e")),
                "semi_major_axis_au": _float(entry.get("a")),
                "perihelion_dist_au": _float(entry.get("q")),
                "aphelion_dist_au": _float(entry.get("ad")),
                "inclination_deg": _float(entry.get("i")),
                "long_asc_node_deg": _float(entry.get("om")),
                "arg_perihelion_deg": _float(entry.get("w")),
                "mean_anomaly_deg": _float(entry.get("ma")),
                "time_perihelion_tdb": entry.get("tp"),
                "orbital_period_days": _float(entry.get("per")),
                "mean_motion_deg_d": _float(entry.get("n")),

                "moid_au": _float(entry.get("moid")),
                "moid_jup": _float(entry.get("moid_jup")),
                "t_jup": _float(entry.get("t_jup")),
                "condition_code": entry.get("condition_code"),
                "data_arc_days": _int(entry.get("data_arc")),
                "n_obs_used": _int(entry.get("n_obs_used")),
                "n_del_obs_used": _int(entry.get("n_del_obs_used")),
                "n_dop_obs_used": _int(entry.get("n_dop_obs_used")),
                "rms": _float(entry.get("rms")),

                "first_obs_date": entry.get("first_obs"),
                "last_obs_date": entry.get("last_obs"),

                "model_pars": None,

                "absolute_magnitude_h": _float(entry.get("H")),
                "magnitude_slope_g": _float(entry.get("G")),
                "diameter_km": _float(entry.get("diameter")),
                "albedo": _float(entry.get("albedo")),
                "rotation_period_h": _float(entry.get("rot_per")),
                "thermal_inertia": None,
                "spectral_type": None,

                # Discovery fields — not available in bulk
                "discovery_date": None,
                "discovery_site": None,
                "discovery_location": None,
                "discovery_who": None,
                "discovery_name": None,
                "discovery_ref": None,
                "discovery_cref": None,
                "discovery_text": None,
                "discovery_citation": None,
            }

            results[pdes] = parsed
            # Also index by spkid for cross-referencing
            if entry.get("spkid"):
                results[str(entry["spkid"])] = parsed
            # Index by name
            name = entry.get("name", "")
            if name:
                results[name.lower()] = parsed

        logger.info(f"[SBDB] Bulk query: {len(data['data'])} NEOs loaded")
        return results

    # ── Per-Asteroid Fetch (sbdb.api) ─────────────────────────

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
