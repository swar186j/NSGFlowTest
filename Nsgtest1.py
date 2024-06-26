"""Azure function collector code for LogScale."""
__version__ = "1.0.1"
# pylint: disable=W0703
import datetime
import logging
import json
import asyncio
import os
import sys
import warnings
import backoff
import requests
import azure.functions as func
from azure.storage.blob import ContainerClient

LOG_BATCHES = 5000
CONTAINER_NAME = "insights-logs-networksecuritygroupflowevent"
CHECKPOINT_FILE = "logscale-checkpoint.json"
warnings.filterwarnings("ignore", category=DeprecationWarning)

class CustomException(Exception):
    """Custom Exception raised while failure occurs."""

class Checkpoint:
    """Checkpoint helper class for azure function code."""

    def __init__(self) -> None:
        """Construct an instance of the class."""
        self.connect_str = os.environ.get("AzureWebJobsStorage")
        self.checkpoint_container = "logscale-checkpoint-store"

    def get_checkpoint(self, checkpoint):
        """Get the checkpoint json from blob storage.

        Args:
            checkpoint (dict): latest checkpoint json

        Returns:
            dict: latest checkpoint json from blob storage
        """
        checkpoint_dict = {}
        try:
            service = ContainerClient.from_connection_string(
                conn_str=self.connect_str, container_name=self.checkpoint_container
            )
            if not service.exists():
                service.create_container()
                checkpoint_str = json.dumps(checkpoint)
                blob_client = service.get_blob_client(blob=CHECKPOINT_FILE)
                blob_client.upload_blob(checkpoint_str, overwrite=True)
                checkpnt = checkpoint
            else:
                blob_client = service.get_blob_client(CHECKPOINT_FILE)
                streamdownloader = blob_client.download_blob()
                checkpoint_dict = json.loads(streamdownloader.readall())
                checkpnt = checkpoint_dict
                logging.info(
                    "CHECKPOINT: starting data fetching from ['blob_name':'last_modified'] %s",
                    checkpoint_dict)
        except Exception as exception:
            logging.error(
                "Exception occurred while obtaining latest checkpoint value %s",
                exception)
        else:
            return checkpnt
        return None

    def update_checkpoint(self, checkpoint):
        """Update the Checkpoint file on blob storage.

        Args:
            checkpoint (dict): latest checkpoint updated
        """
        try:
            checkpoint_str = json.dumps(checkpoint)
            service = ContainerClient.from_connection_string(
                conn_str=self.connect_str, container_name=self.checkpoint_container
            )
            blob_client = service.get_blob_client(blob=CHECKPOINT_FILE)
            blob_client.upload_blob(checkpoint_str, overwrite=True)
        except IOError:
            pass
        except Exception as exception:
            logging.info(
                "Exception occurred while updating checkpoint file to blob storage %s",
                exception)

@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_tries=5,
                      raise_on_giveup=False,
                      max_time=30)
def ingest_to_logscale(records):
    """Ingesting records into LogScale Instance.

    Args:
        records (dict)

    Raises:
        CustomException: Raises Custom
        Exception when Failure occurs
        at status_code 400,401,403,404

    Returns:
        dict: Returns the response json of POST request.
    """
    try:
        logscale_token = os.environ.get("LogScaleIngestToken")
        response = requests.post(
            url=os.environ.get("LogScaleHostURL"),
            headers={
                "Authorization": f"Bearer {logscale_token}",
                "Content-Type": "application/json",
            },
            data=json.dumps(records),
            timeout=30,
        )
        if response.status_code in [400, 401, 403, 404]:
            raise CustomException(
                f"Status-code {response.status_code} Exception {response.text}"
            )
    except CustomException as exception:
        logging.error("%s", exception)
        sys.exit(1)

    except requests.exceptions.RequestException as exception:
        logging.error(
            "Exception occurred while posting to LogScale %s",
            exception)
        raise requests.exceptions.RequestException from exception
    else:
        return response.json()

def main(mytimer: func.TimerRequest) -> None:
    """Begin main routine."""
    utc_timestamp = (
        datetime.datetime.utcnow().replace(
            tzinfo=datetime.timezone.utc).isoformat())

    checkpoint_obj = Checkpoint()
    checkpoint = checkpoint_obj.get_checkpoint({})

    blob_service_client = ContainerClient.from_connection_string(os.environ.get("AzureWebJobsStorage"), CONTAINER_NAME)
    new_checkpoint = checkpoint.copy()

    for blob in blob_service_client.list_blobs():
        blob_client = blob_service_client.get_blob_client(blob)
        blob_properties = blob_client.get_blob_properties()

        if blob.name not in checkpoint or blob_properties['last_modified'] > checkpoint[blob.name]:
            blob_data = blob_client.download_blob().readall()
            records = json.loads(blob_data)

            response = ingest_to_logscale(records)
            if response["text"] == "Success" and response["eventCount"] > 0:
                new_checkpoint[blob.name] = blob_properties['last_modified'].isoformat()
                logging.info(f"Successfully ingested data from {blob.name} to LogScale")

    checkpoint_obj.update_checkpoint(new_checkpoint)

    if mytimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer trigger function executed at %s", utc_timestamp)
