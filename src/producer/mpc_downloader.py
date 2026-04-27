"""
MPC Bulk Data Downloader — fetches NEA.txt and PHA.txt from MPC servers.

Downloads the Minor Planet Center's fixed-width orbit files and
ingests them into the neo_agency_mpc table with full Keplerian elements.
"""

import asyncio
import gzip
import os
from datetime import datetime, timezone

import httpx
import psycopg2
from psycopg2.extras import execute_values

from src.config import Config
from src.logger import logger

# MPC data URLs
MPC_BASE_URL = "https://www.minorplanetcenter.net/iau/MPCORB"
MPC_NEA_URL = f"{MPC_BASE_URL}/NEA.txt"
MPC_PHA_URL = f"{MPC_BASE_URL}/PHA.txt"

# Download directory (project-relative, not /tmp/)
DOWNLOAD_DIR = os.path.join(Config.PROJECT_ROOT, "data", "mpc_downloads")


def get_db_connection():
    return psycopg2.connect(
        host=Config.DB_HOST, port=Config.DB_PORT,
        dbname=Config.DB_NAME, user=Config.DB_USER, password=Config.DB_PASSWORD,
    )


async def download_file(url: str, filename: str) -> str | None:
    """Download a file from MPC server. Returns local path or None."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    local_path = os.path.join(DOWNLOAD_DIR, filename)

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            logger.info(f"Downloading MPC data from {url}")
            resp = await client.get(url, follow_redirects=True)

            if resp.status_code != 200:
                logger.warning(f"Failed to download {filename}: HTTP {resp.status_code}")
                return None

            with open(local_path, "wb") as f:
                f.write(resp.content)

            size = os.path.getsize(local_path)
            logger.info(f"Downloaded {filename}: {size:,} bytes → {local_path}")
            return local_path

    except Exception as e:
        logger.error(f"Error downloading {filename}: {e}")
        return None


def parse_mpc_line(line: str) -> dict | None:
    """
    Parse a line from MPCORB.DAT fixed-width format.

    Column layout (0-indexed):
      0-7    : Number or packed provisional designation
      8-13   : Absolute magnitude H
      15-19  : Slope parameter G
      20-25  : Epoch (packed MPC format)
      26-35  : Mean anomaly (deg)
      37-46  : Argument of perihelion (deg)
      48-57  : Longitude of ascending node (deg)
      59-68  : Inclination (deg)
      70-79  : Eccentricity
      80-91  : Mean daily motion (deg/day)
      92-103 : Semi-major axis (AU)
      105-106: Uncertainty parameter
    """
    if not line or len(line) < 106:
        return None

    try:
        data = {
            "designation": line[0:7].strip(),
            "H": _safe_float(line[8:13].strip()),
            "G": _safe_float(line[15:19].strip()),
            "epoch_packed": line[20:25].strip(),
            "mean_anomaly_deg": _safe_float(line[26:35].strip()),
            "arg_perihelion_deg": _safe_float(line[37:46].strip()),
            "long_asc_node_deg": _safe_float(line[48:57].strip()),
            "inclination_deg": _safe_float(line[59:68].strip()),
            "eccentricity": _safe_float(line[70:79].strip()),
            "mean_daily_motion": _safe_float(line[80:91].strip()),
            "semi_major_axis_au": _safe_float(line[92:103].strip()),
            "uncertainty": line[105:106].strip() if len(line) > 105 else "",
        }

        # Require at least designation + one orbital element
        if not data["designation"]:
            return None
        if data["semi_major_axis_au"] is None and data["eccentricity"] is None:
            return None

        return data
    except (ValueError, IndexError):
        return None


async def ingest_mpc_orbits(file_path: str, source_type: str = "nea"):
    """
    Ingest MPC orbit data into neo_agency_mpc table.

    FIX: Correctly maps columns and uses execute_values for batch insert.
    """
    try:
        if file_path.endswith(".gz"):
            with gzip.open(file_path, "rt") as f:
                lines = f.readlines()
        else:
            with open(file_path, "r") as f:
                lines = f.readlines()

        conn = get_db_connection()
        cur = conn.cursor()

        records = []
        skipped = 0

        # Skip header lines (first 43 lines in MPCORB format)
        for line in lines[43:]:
            parsed = parse_mpc_line(line)
            if not parsed:
                skipped += 1
                continue

            records.append((
                parsed["designation"],       # asteroid_id (use designation as ID)
                "found",                     # status
                parsed["designation"],       # mpc_designation
                None,                        # mpc_name
                None,                        # permid
                None,                        # packed_permid
                None,                        # epoch_mjd (would need epoch decoding)
                parsed["semi_major_axis_au"],
                parsed["eccentricity"],
                parsed["inclination_deg"],
                parsed["long_asc_node_deg"],
                parsed["arg_perihelion_deg"],
                parsed["mean_anomaly_deg"],
            ))

        # Batch upsert
        if records:
            sql = """
                INSERT INTO neo_agency_mpc (
                    asteroid_id, status, mpc_designation, mpc_name,
                    permid, packed_permid, epoch_mjd,
                    semi_major_axis_au, eccentricity, inclination_deg,
                    long_asc_node_deg, arg_perihelion_deg, mean_anomaly_deg,
                    ingestion_time
                ) VALUES %s
                ON CONFLICT (asteroid_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    mpc_designation = EXCLUDED.mpc_designation,
                    semi_major_axis_au = EXCLUDED.semi_major_axis_au,
                    eccentricity = EXCLUDED.eccentricity,
                    inclination_deg = EXCLUDED.inclination_deg,
                    long_asc_node_deg = EXCLUDED.long_asc_node_deg,
                    arg_perihelion_deg = EXCLUDED.arg_perihelion_deg,
                    mean_anomaly_deg = EXCLUDED.mean_anomaly_deg,
                    ingestion_time = now()
            """
            # Add ingestion_time placeholder
            full_records = [r + (datetime.now(timezone.utc),) for r in records]
            execute_values(cur, sql, full_records, page_size=500)
            conn.commit()
            logger.info(f"Ingested {len(records)} MPC {source_type.upper()} records ({skipped} skipped)")

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error ingesting MPC orbits: {e}", exc_info=True)


async def fetch_mpc_data():
    """Main function to download and ingest MPC bulk data."""
    logger.info("Starting MPC bulk data ingestion")

    # Download and ingest NEA data
    nea_path = await download_file(MPC_NEA_URL, "NEA.txt")
    if nea_path:
        await ingest_mpc_orbits(nea_path, source_type="nea")

    # Download and ingest PHA data
    pha_path = await download_file(MPC_PHA_URL, "PHA.txt")
    if pha_path:
        await ingest_mpc_orbits(pha_path, source_type="pha")

    logger.info("MPC bulk data ingestion complete")


def _safe_float(val) -> float | None:
    if not val:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    asyncio.run(fetch_mpc_data())
