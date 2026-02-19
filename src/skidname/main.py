#!/usr/bin/env python
# * coding: utf8 *
"""
Run the SKIDNAME script as a cloud function.
"""

import base64
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import arcgis
import functions_framework
from cloudevents.http import CloudEvent
from palletjack import extract, load, transform, utils
from supervisor.message_handlers import SendGridHandler
from supervisor.models import MessageDetails, Supervisor

#: This makes it work when calling with just `python <file>`/installing via pip and in the gcf framework, where
#: the relative imports fail because of how it's calling the function.
try:
    from . import config, version
except ImportError:
    import config
    import version


def _get_secrets():
    """A helper method for loading secrets from either a GCF mount point or the local src/skidname/secrets/secrets.json file

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

        #########################################################################
        #: Use the various palletjack classes and other code to do your work here
        #########################################################################

        module_logger.info("Log messages with module_logger.info() or module_logger.debug()")

        #: Create a extract object to load your new data
        extractor = extract.PostgresLoader("host", "database", "user", "password")
        new_data_df = extractor.read_table_into_dataframe("table_name", "index_column", "crs", "shape_column")

        #: Transform your data
        new_data_df = new_data_df["new_column"] = "do custom transform stuff here"
        new_data_df = transform.DataCleaning.rename_dataframe_columns_for_agol(new_data_df)

        #: Use retry for operations that may fail randomly (network issues, etc)
        utils.retry("method_to_retry", "arg1", keyword_arg="arg2")

        #: Create a load object to load your new data
        loader = load.FeatureServiceUpdater(gis, "item_id")
        loader.update_features(new_data_df)

        end = datetime.now()

        summary_message = MessageDetails()
        summary_message.subject = f"{config.SKID_NAME} Update Summary"
        summary_rows = [
            f"{config.SKID_NAME} update {start.strftime('%Y-%m-%d')}",
            "=" * 20,
            "",
            f"Start time: {start.strftime('%H:%M:%S')}",
            f"End time: {end.strftime('%H:%M:%S')}",
            f"Duration: {str(end - start)}",
            #: Add other rows here containing summary info captured/calculated during the working portion of the skid,
            #: like the number of rows updated or the number of successful attachment overwrites.
        ]

        summary_message.message = "\n".join(summary_rows)
        summary_message.attachments = tempdir_path / log_name

        skid_supervisor.notify(summary_message)

        #: Remove file handler so the tempdir will close properly
        loggers = [logging.getLogger(config.SKID_NAME), logging.getLogger("palletjack")]
        _remove_log_file_handlers(log_name, loggers)


@functions_framework.cloud_event
def subscribe(cloud_event: CloudEvent) -> None:
    """Entry point for Google Cloud Function triggered by pub/sub event

    Args:
         cloud_event (CloudEvent):  The CloudEvent object with data specific to this type of
                        event. The `type` field maps to
                         `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
                        The `data` field maps to the PubsubMessage data
                        in a base64-encoded string. The `attributes` field maps
                        to the PubsubMessage attributes if any is present.
    Returns:
        None. The output is written to Cloud Logging.
    """

    #: This function must be called 'subscribe' to act as the Google Cloud Function entry point. It must accept the
    #: CloudEvent object as the only argument.

    #: You can get the message-body value from the Cloud Scheduler event sent via pub/sub to customize the run
    if base64.b64decode(cloud_event.data["message"]["data"]).decode() == "foo":
        pass

    #: Call process() and any other functions you want to be run as part of the skid here.
    process()


#: Putting this here means you can call the file via `python main.py` and it will run. Useful for pre-GCF testing.
if __name__ == "__main__":
    process()
