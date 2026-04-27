"""
ESA NEO Coordination Centre (NEOCC) Client

Portal  : https://neo.ssa.esa.int/
Risk API: https://neo.ssa.esa.int/PSDB-portlet/download?file=esa_risk_list
          Returns pipe-delimited text — NOT JSON.

Live format verified (2026-04-27):
  Header line 1: "Last Update: 2026-04-26 14:04 UTC"
  Header line 2: column labels
  Header line 3: column label row 2
  Header line 4: column format mask
  Data lines:  Num/des   Name  | Diameter_m | *=Y | Date/Time | IP_max | PS_max | TS | Vel_km_s | Years | IP_cum | PS_cum
  Delimiter: pipe (|) with spaces

Column positions (0-indexed, space-stripped):
  0: Num/des + Name (combined in first field, split on whitespace)
  1: Diameter_m
  2: diameter_certain (* = Y)
  3: VI_date (YYYY-MM-DD HH:MM)
  4: ip_max
  5: ps_max
  6: ts
  7: vel_km_s
  8: years range
  9: ip_cum
  10: ps_cum
"""

from __future__ import annotations
import re
from src.agencies.base import BaseClient, FetchStatus
from src.logger import logger


ESA_RISK_URL = "https://neo.ssa.esa.int/PSDB-portlet/download?file=esa_risk_list"
ESA_SPECIAL_RISK_URL = "https://neo.ssa.esa.int/PSDB-portlet/download?file=esa_special_risk_list"


class ESAClient(BaseClient):
    """ESA NEOCC — risk list via pipe-delimited file download."""

    def __init__(self, http_client=None, semaphore=None):
        super().__init__(
            name="ESA_NEOCC",
            base_url="https://neo.ssa.esa.int",
            rate_limit=1.0,
            http_client=http_client,
            semaphore=semaphore,
        )
        self._risk_cache: dict[str, dict] | None = None

    async def refresh_risk_list(self) -> tuple[dict[str, dict] | None, FetchStatus]:
        """
        Download and parse the full ESA risk list (pipe-delimited).
        Merges regular + special risk lists.
        Returns a dict keyed by normalised designation.
        """
        try:
            text = await self._get_text(ESA_RISK_URL)
            if not text or len(text.strip()) < 100:
                logger.warning("[ESA] Risk list unavailable or empty")
                self._risk_cache = {}
                return None, FetchStatus("ESA_NEOCC", 404, "unavailable")

            entries = self._parse_risk_list(text)

            # Also fetch special risk list (Bennu, Didymos etc.)
            try:
                special_text = await self._get_text(ESA_SPECIAL_RISK_URL)
                if special_text:
                    special = self._parse_risk_list(special_text)
                    entries.update(special)
            except Exception:
                pass  # Special list is optional

            self._risk_cache = entries
            logger.info(f"[ESA] Loaded {len(entries)} objects from risk list")
            return entries, FetchStatus("ESA_NEOCC", 200)

        except Exception as exc:
            logger.warning(f"[ESA] Failed to fetch risk list: {exc}")
            self._risk_cache = {}
            return None, FetchStatus("ESA_NEOCC", None, type(exc).__name__)

    async def check_risk(self, designation: str) -> tuple[dict | None, FetchStatus]:
        """Check if a specific object is on the ESA risk list."""
        if self._risk_cache is None:
            await self.refresh_risk_list()

        if self._risk_cache is None:
            return None, FetchStatus("ESA_NEOCC", None, "cache_unavailable")

        norm = _normalise(designation)
        entry = self._risk_cache.get(norm)

        if entry is None:
            # Fuzzy match: try without year, numbered variants
            for key, val in self._risk_cache.items():
                if norm in key or key in norm:
                    entry = val
                    break

        if entry:
            return {"on_risk_list": True, **entry}, FetchStatus("ESA_NEOCC", 200)

        return {"on_risk_list": False}, FetchStatus("ESA_NEOCC", 200)

    # ── Parsing ──────────────────────────────────────────────

    @staticmethod
    def _parse_risk_list(text: str) -> dict[str, dict]:
        """
        Parse ESA pipe-delimited risk list.

        Line format (after skipping 4 header lines):
          '2023VD3                       |   14 |    *    | 2034-11-08 17:08 |  2.35E-3 |  -2.67 |  0 |   21.01  | 2034-2039 |  2.35E-3 |  -2.67 |'

        First field is "Num/des  Name" (fixed-width ~30 chars).
        """
        entries: dict[str, dict] = {}
        lines = text.strip().split("\n")

        # Skip header lines — find first data line (not starting with 'Last', spaces, or 'A')
        data_lines = []
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            # Skip header lines (contain only A/N chars in format masks, or 'Last Update', or labels)
            if (stripped.startswith("Last") or
                    stripped.startswith("Num") or
                    stripped.startswith("AAAA") or
                    stripped.startswith("Object")):
                continue
            data_lines.append(stripped)

        for line in data_lines:
            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 5:
                continue

            # First field: "Num/des  Name" — designation is first token
            first_field = parts[0].strip()
            tokens = first_field.split()
            if not tokens:
                continue

            # Separate designation from name
            # Numbered: "101955  Bennu" → des="101955", name="Bennu"
            # Provisional: "2023VD3  " → des="2023VD3", name=""
            if len(tokens) >= 2 and tokens[0].isdigit():
                esa_designation = tokens[0]
                esa_name = " ".join(tokens[1:])
            else:
                esa_designation = tokens[0]
                esa_name = " ".join(tokens[1:]) if len(tokens) > 1 else ""

            norm = _normalise(esa_designation)

            try:
                diameter_m = _safe_float(parts[1]) if len(parts) > 1 else None
                diameter_certain = ("*" in parts[2]) if len(parts) > 2 else False
                vi_date = parts[3].strip() if len(parts) > 3 else None
                ip_max = _safe_sci(parts[4]) if len(parts) > 4 else None
                ps_max = _safe_float(parts[5]) if len(parts) > 5 else None
                ts = _safe_int(parts[6]) if len(parts) > 6 else None
                vel_km_s = _safe_float(parts[7]) if len(parts) > 7 else None
                years = parts[8].strip() if len(parts) > 8 else None
                ip_cum = _safe_sci(parts[9]) if len(parts) > 9 else None
                ps_cum = _safe_float(parts[10]) if len(parts) > 10 else None
            except Exception:
                continue

            entries[norm] = {
                "esa_designation": esa_designation,
                "esa_name": esa_name if esa_name else None,
                "diameter_m": diameter_m,
                "diameter_certain": diameter_certain,
                "vi_date": vi_date,
                "ip_max": ip_max,
                "ps_max": ps_max,
                "ts": ts,
                "vel_km_s": vel_km_s,
                "years": years,
                "ip_cum": ip_cum,
                "ps_cum": ps_cum,
            }

        return entries


# ── Helpers ───────────────────────────────────────────────────

def _normalise(designation: str) -> str:
    s = designation.strip()
    s = re.sub(r"[()]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.lower().strip()


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None


def _safe_sci(val) -> float | None:
    """Parse scientific notation like '2.35E-3'."""
    if val is None:
        return None
    try:
        return float(str(val).strip().replace("E", "e"))
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    if val is None:
        return None
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return None
