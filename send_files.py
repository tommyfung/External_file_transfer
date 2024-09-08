import paramiko
import io
import os
import logging
from azure.storage.blob import BlobServiceClient
from google.auth import default
from google.cloud import storage
from datetime import datetime
from azure.identity import DefaultAzureCredential

def send_files(config, credential):
    hostname = config['send']['sftp']['hostname']
    port = config['send']['sftp']['port']
    username = config['send']['sftp']['username']
    password = config['send']['sftp']['password']
    remote_path = config['send']['sftp']['remote_path']
    use = config['use']

    sent_files = []
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
            files = config['send']['blob']['files']
            blob_service_client = BlobServiceClient(account_url=config['send']['blob']['account_url'], credential=credential)
            container_name = config['send']['blob']['container_name']

            for file_name in files:
                try:
                    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
                    file_obj = io.BytesIO()
                    blob_client.download_blob().readinto(file_obj)
                    file_obj.seek(0)

                    # Upload the file to the SFTP server
                    remote_file_path = os.path.join(remote_path, file_name)
                    sftp.putfo(file_obj, remote_file_path)

                    logging.info(f"[{timestamp}] File '{file_name}' uploaded to SFTP server '{hostname}:{port}'")
                    sent_files.append(file_name)
                except Exception as e:
                    logging.warning(f"[{timestamp}] File '{file_name}' not found: {e}")
                    not_found_files.append(file_name)

        elif use == 'gcp':
            # GCP storage client
            files = config['send']['gcp']['files']
            credentials, project = default()
            storage_client = storage.Client(credentials=credentials, project=project)
            bucket_name = config['send']['gcp']['bucket_name']
            bucket = storage_client.bucket(bucket_name)

            for file_name in files:
                try:
                    blob = bucket.blob(file_name)
                    file_obj = io.BytesIO()
                    blob.download_to_file(file_obj)
                    file_obj.seek(0)

                    # Upload the file to the SFTP server
                    remote_file_path = os.path.join(remote_path, file_name)
                    sftp.putfo(file_obj, remote_file_path)

                    logging.info(f"[{timestamp}] File '{file_name}' uploaded to SFTP server '{hostname}:{port}'")
                    sent_files.append(file_name)
                except Exception as e:
                    logging.warning(f"[{timestamp}] File '{file_name}' not found: {e}")
                    not_found_files.append(file_name)

        sftp.close()
        transport.close()

        return {
            "message": "Files processed.",
            "sent_files": sent_files,
            "not_found_files": not_found_files
        }
    
    except paramiko.SSHException as ssh_error:
        logging.error(f"[{timestamp}] SFTP connection error: {ssh_error}")
        return {
            "message": f"SFTP connection error: {ssh_error}",
            "error_code": 1,
            "sent_files": sent_files,
            "not_found_files": not_found_files
        }
    except Exception as e:
        logging.error(f"[{timestamp}] An error occurred: {e}")
        return {
            "message": f"An error occurred: {e}",
            "error_code": 2,
            "sent_files": sent_files,
            "not_found_files": not_found_files
        }
