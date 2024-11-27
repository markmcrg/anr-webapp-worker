from b2sdk.v2 import InMemoryAccountInfo, B2Api
from tabulate import tabulate  # For table formatting
import schedule  # For scheduling
import threading
import time
import datetime
import os


def authenticate_b2(bucket_name):
    """Authenticate with B2 and return the specified bucket."""
    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    application_key_id = os.environ['b2_keyID']
    application_key = os.environ['b2_applicationKey']
    b2_api.authorize_account("production", application_key_id, application_key)
    bucket = b2_api.get_bucket_by_name(bucket_name)
    return bucket


def delete_empty_files(bucket):
    """Find and delete files with 0 bytes in the specified bucket."""
    print("Running job to delete files with 0 bytes at", datetime.datetime.now())
    empty_files = []

    for file_version, _ in bucket.ls(latest_only=False, recursive=True):
        if file_version.size == 0:
            empty_files.append({
                "File Name": file_version.file_name,
                "Size (bytes)": file_version.size,
                "File ID": file_version.id_,
            })

    if empty_files:
        # Print in table format
        print("\nFiles with 0 bytes:")
        print(tabulate(empty_files, headers="keys", tablefmt="grid"))
        
        # Delete files
        for file in empty_files:
            bucket.delete_file_version(file['File ID'], file['File Name'])
            print(f"Deleted {file['File Name']} successfully!")
    else:
        print("No files with 0 bytes found at", datetime.datetime.now())


def job():
    """Scheduled job to clean up 0-byte files."""
    bucket = authenticate_b2('anr-webapp')
    delete_empty_files(bucket)


def listen_for_force_delete():
    """Listen for user input to trigger an immediate cleanup."""
    bucket = authenticate_b2('anr-webapp')
    while True:
        user_input = input("Type 'force' to delete 0-byte files immediately: ").strip().lower()
        if user_input == 'force':
            print("Force delete triggered.")
            delete_empty_files(bucket)


# Schedule the job to run every 10 minutes
schedule.every(10).minutes.do(job)

# Start the force-delete listener in a separate thread
listener_thread = threading.Thread(target=listen_for_force_delete, daemon=True)
listener_thread.start()

# Run the scheduled job loop
print("Scheduled job started. Waiting for execution...")
while True:
    schedule.run_pending()
    time.sleep(1)
