import logging
import os
import json
import requests
from azure.storage.blob import BlobServiceClient
import azure.functions as func

def main(myblob: func.InputStream, context: func.Context) -> func.HttpResponse:
    logging.info("Python blob trigger function processed a request.")

    # Retrieve environment variables securely using DefaultAzureCredential
    logscale_url = os.getenv("LOGSCALE_URL")
    bearer_token = os.getenv("LOGSCALE_BEARER_TOKEN")

    if not logscale_url or not bearer_token:
        logging.error("Missing environment variables: LOGSCALE_URL and LOGSCALE_BEARER_TOKEN")
        return func.HttpResponse(
            "Error: Please set LOGSCALE_URL and LOGSCALE_BEARER_TOKEN environment variables.",
            status_code=500
        )

    # Connect to Azure Blob Storage using DefaultAzureCredential
    try:
        blob_service_client = BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])
        container_client = blob_service_client.get_container_client("your-container-name")  # Replace with your container name
    except Exception as e:
        logging.error(f"Error connecting to Azure Blob Storage: {e}")
        return func.HttpResponse(
            "Error: Failed to connect to Azure Blob Storage.",
            status_code=500
        )

    # Get blob name from the trigger metadata (if available)
    blob_name = myblob.name
    if not blob_name:
        logging.warning("Blob name not found in trigger metadata.")
        return func.HttpResponse(
            "Warning: Blob name not available. Processing blob data without name.",
            status_code=200
        )

    # Download blob data
    try:
        download_stream = myblob.readall()
    except Exception as e:
        logging.error(f"Error downloading blob data: {e}")
        return func.HttpResponse(
            "Error: Failed to download blob data.",
            status_code=500
        )

    # Parse blob data (assuming JSON format)
    try:
        data = json.loads(download_stream.decode())
    except Exception as e:
        logging.error(f"Error parsing blob data as JSON: {e}")
        return func.HttpResponse(
            "Warning: Failed to parse blob data as JSON. Sending raw data.",
            status_code=200
        )
    else:
        logging.info("Successfully parsed blob data as JSON.")

    # Send data to Logscale using POST request with bearer token
    headers = {"Authorization": f"Bearer {bearer_token}"}
    try:
        response = requests.post(logscale_url, json=data, headers=headers)
        response.raise_for_status()  # Raise exception for non-2xx status codes
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending data to Logscale: {e}")
        return func.HttpResponse(
            "Error: Failed to send data to Logscale.",
            status_code=500
        )

    logging.info(f"Successfully sent data to Logscale. Response: {response.text}")
    return func.HttpResponse("Data sent to Logscale successfully.", status_code=200)
  
