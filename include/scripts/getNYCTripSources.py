import requests
from google.cloud import storage
import os

def download_file(url, local_filename):
    """
    Downloads a file from a URL and saves it locally.
    """
    response = requests.get(url)
    response.raise_for_status()  # Check if the request was successful

    with open(local_filename, 'wb') as file:
        file.write(response.content)

    return local_filename

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """
    Uploads a file to Google Cloud Storage.
    """
    # Initialize a GCS client
    storage_client = storage.Client()
    
    # Get the bucket
    bucket = storage_client.bucket(bucket_name)
    
    # Create a new blob and upload the file's content
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def main():
    # Define the URL of the file to download
    file_url = 'https://example.com/path/to/your/file'
    
    # Define the local file name to save the downloaded file
    local_filename = 'downloaded_file.ext'
    
    # Define your GCS bucket name and destination blob name
    bucket_name = 'your-gcs-bucket-name'
    destination_blob_name = 'path/in/gcs/to/save/file'

    # Download the file
    print(f"Downloading file from {file_url}...")
    downloaded_file = download_file(file_url, local_filename)
    print(f"File downloaded and saved as {downloaded_file}.")

    # Upload the file to GCS
    print(f"Uploading file to GCS bucket {bucket_name}...")
    upload_to_gcs(bucket_name, downloaded_file, destination_blob_name)
    print("Upload complete.")

if __name__ == "__main__":
    main()
