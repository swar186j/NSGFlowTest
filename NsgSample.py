import hashlib
import logging
import os
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.identity import DefaultAzureCredential
from azure.eventhub import EventHubProducerClient, EventData
from azure.core.exceptions import ResourceNotFoundError
from datetime import datetime

# Initialize Event Hub client
event_hub_connection_string = os.getenv("EVENT_HUB_CONNECTION_STRING")
event_hub_name = os.getenv("EVENT_HUB_NAME")
producer = EventHubProducerClient.from_connection_string(
    conn_str=event_hub_connection_string, eventhub_name=event_hub_name
)

# Initialize Blob Service Client
storage_account_url = os.getenv("STORAGE_ACCOUNT_URL")
container_name = os.getenv("CONTAINER_NAME")
blob_service_client = BlobServiceClient(account_url=storage_account_url, credential=DefaultAzureCredential())
container_client = blob_service_client.get_container_client(container_name)

def main(mytimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function started.')

    # Get the current hour to process only current hour logs
    current_hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    folder_path = f"insights-logs-networksecuritygroupflowevent/{current_hour.strftime('%Y/%m/%d/%H')}/"

    try:
        # List blobs in the folder
        blob_list = container_client.list_blobs(name_starts_with=folder_path)

        for blob in blob_list:
            logging.info(f"Processing blob: {blob.name}")
            blob_client = container_client.get_blob_client(blob.name)

            # Download blob content
            blob_data = blob_client.download_blob().readall().decode('utf-8')
            entries = blob_data.split('\n')

            for entry in entries:
                if entry.strip():
                    entry_hash = hashlib.md5(entry.encode()).hexdigest()
                    if not entry_exists(entry_hash):
                        send_to_event_hub(entry)
                        store_entry_hash(entry_hash)
                    else:
                        logging.info(f"Duplicate entry found: {entry_hash}")

    except Exception as e:
        logging.error(f"Error processing blobs: {e}")

    logging.info('Python timer trigger function completed.')

def entry_exists(entry_hash):
    table_client = blob_service_client.get_table_client("ProcessedEntries")
    try:
        entity = table_client.get_entity(partition_key="EntryHashes", row_key=entry_hash)
        return True
    except ResourceNotFoundError:
        return False

def store_entry_hash(entry_hash):
    table_client = blob_service_client.get_table_client("ProcessedEntries")
    entity = {
        'PartitionKey': 'EntryHashes',
        'RowKey': entry_hash,
        'Timestamp': datetime.utcnow().isoformat()
    }
    table_client.upsert_entity(entity)

def send_to_event_hub(entry):
    event_data = EventData(entry)
    with producer:
        producer.send_batch([event_data])
    logging.info("Successfully sent log entry to Event Hub.")
