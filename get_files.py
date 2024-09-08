import paramiko
import io
import os
import logging
from azure.storage.blob import BlobServiceClient
from google.auth import default
from google.cloud import storage
from datetime import datetime
from azure.identity import DefaultAzureCredential

def get_files(config, credential):
    hostname = config['get']['sftp']['hostname']
    port = config['get']['sftp']['port']
    username = config['get']['sftp']['username']
    password = config['get']['sftp']['password']
    remote_path = config['get']['sftp']['remote_path']
    files = config['get']['sftp']['files']
    use = config['use']

    downloaded_files = []
    not_found_files = []

    try:
        # Timestamp for logging
        timestamp = datetime.utcnow().isoformat()

        # Connect to the SFTP server
        transport = paramiko.Transport((hostname, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        if use == 'azure':
            # Blob service client
            blob_service_client = BlobServiceClient(account_url=config['get']['blob']['account_url'], credential=credential)
            container_name = config['get']['blob']['container_name']

            for file_name in files:
                try:
                    remote_file_path = os.path.join(remote_path, file_name)
                    file_obj = io.BytesIO()
                    sftp.getfo(remote_file_path, file_obj)
                    file_obj.seek(0)

                    # Upload the file to Blob Storage
                    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
                    blob_client.upload_blob(file_obj, overwrite=True)

                    logging.info(f"[{timestamp}] File '{file_name}' uploaded to Azure Blob Storage container '{container_name}'")
                    downloaded_files.append(file_name)
                except Exception as e:
                    logging.warning(f"[{timestamp}] File '{file_name}' not found: {e}")
                    not_found_files.append(file_name)

        elif use == 'gcp':
            # GCP storage client
            credentials, project = default()
            storage_client = storage.Client(credentials=credentials, project=project)
            bucket_name = config['get']['gcp']['bucket_name']
            bucket = storage_client.bucket(bucket_name)

            for file_name in files:
                try:
                    remote_file_path = os.path.join(remote_path, file_name)
                    file_obj = io.BytesIO()
                    sftp.getfo(remote_file_path, file_obj)
                    file_obj.seek(0)

                    # Upload the file to GCP Bucket
                    blob = bucket.blob(file_name)
                    blob.upload_from_file(file_obj)

                    logging.info(f"[{timestamp}] File '{file_name}' uploaded to GCP bucket '{bucket_name}'")
                    downloaded_files.append(file_name)
                except Exception as e:
                    logging.warning(f"[{timestamp}] File '{file_name}' not found: {e}")
                    not_found_files.append(file_name)

        sftp.close()
        transport.close()

        return {
            "message": "Files processed.",
            "downloaded_files": downloaded_files,
            "not_found_files": not_found_files
        }
    
    except paramiko.SSHException as ssh_error:
        logging.error(f"[{timestamp}] SFTP connection error: {ssh_error}")
        return {
            "message": f"SFTP connection error: {ssh_error}",
            "error_code": 1,
            "downloaded_files": downloaded_files,
            "not_found_files": not_found_files
        }
    except Exception as e:
        logging.error(f"[{timestamp}] An error occurred: {e}")
        return {
            "message": f"An error occurred: {e}",
            "error_code": 2,
            "downloaded_files": downloaded_files,
            "not_found_files": not_found_files
        }
