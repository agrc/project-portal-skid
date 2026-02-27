#!/usr/bin/env python
# * coding: utf8 *
"""
Run the project_portal_skid script as a cloud function.
"""

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import arcgis
import geopandas as gpd
import pandas as pd
import requests
from palletjack import load, transform
from shapely.geometry import Point
from supervisor.message_handlers import SendGridHandler
from supervisor.models import MessageDetails, Supervisor

from . import config, version


def _get_secrets():
    """A helper method for loading secrets from either a GCF mount point or the local src/project_portal_skid/secrets/secrets.json file

    Raises:
        FileNotFoundError: If the secrets file can't be found.

    Returns:
        dict: The secrets .json loaded as a dictionary
    """

    secret_folder = Path("/secrets")

    #: Try to get the secrets from the Cloud Function mount point
    if secret_folder.exists():
        return json.loads(Path("/secrets/app/secrets.json").read_text(encoding="utf-8"))

    #: Otherwise, try to load a local copy for local development
    secret_folder = Path(__file__).parent / "secrets"
    if secret_folder.exists():
        return json.loads((secret_folder / "secrets.json").read_text(encoding="utf-8"))

    raise FileNotFoundError("Secrets folder not found; secrets not loaded.")


def _fetch_projects(
    api_key: str, base_url: str = "https://api.upp.utah.gov/beta/projects", page_size: int = 10000
) -> List[Dict[str, Any]]:
    """Fetch all project records (dicts) from the Utah Project Portal API.

    This returns the raw list of project dictionaries and handles cursor-based
    pagination using `nextSearchAfter`.
    """

    if page_size is None or page_size <= 0:
        page_size = 10000
    page_size = min(page_size, 10000)

    session = requests.Session()
    headers = {"x-api-key": api_key}
    params: Dict[str, Any] = {"pageSize": page_size}

    all_projects: List[Dict[str, Any]] = []
    search_after: Optional[str] = None

    while True:
        if search_after:
            params["searchAfter"] = search_after
        try:
            resp = session.get(base_url, headers=headers, params=params, timeout=30)
        except requests.RequestException:
            raise

        # Handle rate limiting simply by sleeping and retrying
        if resp.status_code == 429:
            time.sleep(1)
            continue

        resp.raise_for_status()

        data = resp.json()
        page_projects = data.get("projects") or data.get("results") or []

        if not isinstance(page_projects, list):
            raise ValueError("Unexpected API response format: 'projects' is not a list")

        all_projects.extend(page_projects)

        search_after = data.get("nextSearchAfter")
        if not search_after:
            break

    return all_projects


def _make_point(row: Dict[str, Any]) -> Optional[Point]:
    """Create a Point from a project record's `locationGeoPoint`.

    Returns None if location is missing or invalid.
    """
    loc = row.get("locationGeoPoint") if hasattr(row, "get") else None
    if not loc or not isinstance(loc, dict):
        return None
    lat = loc.get("lat")
    lon = loc.get("lon")
    if lat is None or lon is None:
        return None
    try:
        return Point(lon, lat)
    except Exception:
        return None


def _projects_to_gdf(projects: List[Dict[str, Any]]) -> gpd.GeoDataFrame:
    """Convert a list of project dicts into a GeoDataFrame with Point geometry.

    Records without a valid `locationGeoPoint` will have null geometry.
    """

    df = pd.DataFrame(projects)

    if not df.empty:
        geometry = df.apply(lambda r: _make_point(r), axis=1)
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    else:
        gdf = gpd.GeoDataFrame(df, geometry=pd.Series(dtype="object"), crs="EPSG:4326")

    return gdf


def _replace_null_geometries(gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, int]:
    """Replace null geometries in a GeoDataFrame with a Point at (0, 0).

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        The GeoDataFrame whose null geometries will be replaced.

    Returns
    -------
    gpd.GeoDataFrame
        The GeoDataFrame with null geometries replaced by Point(0, 0).
    int
        The count of geometries that were null and replaced.
    """

    null_mask = gdf.geometry.isna()
    null_count = null_mask.sum()
    if null_count:
        logging.getLogger(config.SKID_NAME).debug("Replacing %d null geometry(ies) with Point(0, 0)", null_count)
        gdf = gdf.copy()
        gdf.loc[null_mask, gdf.geometry.name] = Point(0, 0)

    return gdf, null_count


def _initialize(log_path, sendgrid_api_key):
    """A helper method to set up logging and supervisor

    Args:
        log_path (Path): File path for the logfile to be written
        sendgrid_api_key (str): The API key for sendgrid for this particular application

    Returns:
        Supervisor: The supervisor object used for sending messages
    """

    skid_logger = logging.getLogger(config.SKID_NAME)
    skid_logger.setLevel(config.LOG_LEVEL)
    palletjack_logger = logging.getLogger("palletjack")
    palletjack_logger.setLevel(config.LOG_LEVEL)

    cli_handler = logging.StreamHandler(sys.stdout)
    cli_handler.setLevel(config.LOG_LEVEL)
    formatter = logging.Formatter(
        fmt="%(levelname)-7s %(asctime)s %(name)15s:%(lineno)5s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    cli_handler.setFormatter(formatter)

    log_handler = logging.FileHandler(log_path, mode="w")
    log_handler.setLevel(config.LOG_LEVEL)
    log_handler.setFormatter(formatter)

    skid_logger.addHandler(cli_handler)
    skid_logger.addHandler(log_handler)
    palletjack_logger.addHandler(cli_handler)
    palletjack_logger.addHandler(log_handler)

    #: Log any warnings at logging.WARNING
    #: Put after everything else to prevent creating a duplicate, default formatter
    #: (all log messages were duplicated if put at beginning)
    logging.captureWarnings(True)

    skid_logger.debug("Creating Supervisor object")
    skid_supervisor = Supervisor(handle_errors=False)
    sendgrid_settings = config.SENDGRID_SETTINGS
    sendgrid_settings["api_key"] = sendgrid_api_key
    skid_supervisor.add_message_handler(
        SendGridHandler(
            sendgrid_settings=sendgrid_settings, client_name=config.SKID_NAME, client_version=version.__version__
        )
    )

    return skid_supervisor


def _remove_log_file_handlers(log_name, loggers):
    """A helper function to remove the file handlers so the tempdir will close correctly

    Args:
        log_name (str): The logfiles filename
        loggers (List<str>): The loggers that are writing to log_name
    """

    for logger in loggers:
        for handler in logger.handlers:
            try:
                if log_name in handler.stream.name:
                    logger.removeHandler(handler)
                    handler.close()
            except Exception:
                pass


def process():
    """The main function that does all the work."""

    #: Set up secrets, tempdir, supervisor, and logging
    start = datetime.now()

    secrets = SimpleNamespace(**_get_secrets())

    with TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        log_name = f"{config.LOG_FILE_NAME}_{start.strftime('%Y%m%d-%H%M%S')}.txt"
        log_path = tempdir_path / log_name

        skid_supervisor = _initialize(log_path, secrets.SENDGRID_API_KEY)
        module_logger = logging.getLogger(config.SKID_NAME)

        #: Get our GIS object via the ArcGIS API for Python
        gis = arcgis.gis.GIS(config.AGOL_ORG, secrets.AGOL_USER, secrets.AGOL_PASSWORD)

        raw_projects = _fetch_projects(secrets.PROJECT_PORTAL_API_KEY)
        projects_gdf = _projects_to_gdf(raw_projects)
        module_logger.info("Loaded %d projects", len(projects_gdf))

        #: Transform your data
        new_data_gdf, null_count = _replace_null_geometries(projects_gdf)
        new_data_gdf = transform.DataCleaning.rename_dataframe_columns_for_agol(new_data_gdf)
        new_data_gdf = transform.DataCleaning.switch_to_datetime(new_data_gdf, ["dateCreated", "dateModified"])

        new_data_gdf.drop(columns=["clientId", "clientGroupId", "programIds", "locationGeoPoint"], inplace=True)
        new_data_gdf.rename(columns={"geometry": "SHAPE"}, inplace=True)
        new_data_gdf.set_geometry("SHAPE", inplace=True)

        #: Create a load object to load your new data
        loader = load.ServiceUpdater(gis, secrets.PROJECT_PORTAL_DATA_ITEMID, working_dir=tempdir_path)
        loader.truncate_and_load(new_data_gdf)

        end = datetime.now()

        summary_message = MessageDetails()
        summary_message.subject = "Update Summary"
        summary_rows = [
            f"{config.SKID_NAME} update {start.strftime('%Y-%m-%d')}",
            "=" * 20,
            "",
            f"Start time: {start.strftime('%H:%M:%S')}",
            f"End time: {end.strftime('%H:%M:%S')}",
            f"Duration: {str(end - start)}",
            f"Total projects loaded: {len(projects_gdf)}",
            f"Empty geometries sent to Null Island: {null_count}",
        ]

        summary_message.message = "\n".join(summary_rows)
        summary_message.attachments = tempdir_path / log_name

        skid_supervisor.notify(summary_message)

        #: Remove file handler so the tempdir will close properly
        loggers = [logging.getLogger(config.SKID_NAME), logging.getLogger("palletjack")]
        _remove_log_file_handlers(log_name, loggers)


#: Putting this here means you can call the file via `python main.py` and it will run. Useful for pre-GCF testing.
if __name__ == "__main__":
    process()
