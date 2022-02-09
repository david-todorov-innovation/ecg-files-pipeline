from google.cloud import storage


def blob_exists(bucket_name, file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    return blob.exists()


def rename_blob(bucket_name, blob_name, new_name):
    """Renames a blob."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The ID of the GCS object to rename
    # blob_name = "your-object-name"
    # The new ID of the GCS object
    # new_name = "new-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    new_blob = bucket.rename_blob(blob, new_name)

    print("Blob {} has been renamed to {}".format(blob.name, new_blob.name))


def delete_all_blobs(bucket_name):
    my_storage = storage.Client()
    bucket = my_storage.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    for blob in blobs:
        blob.delete()


def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

    print("Blob {} deleted.".format(blob_name))


def get_file_reader(bucket_name, file_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(file_name)
    reader = blob.open("r")
    return reader


def get_file_writer(bucket_name, file_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    writer = blob.open("w")
    return writer

def remove_timestamp(line):
    words = line.split(",")
    return words[1]


def change_file_extenstion(csv_file_name):
    return csv_file_name.replace("csv", "ecg")


def convert_to_ecg(csv_file_reader, ecg_file_writer):
    while True:
        line = csv_file_reader.readline()

        if not line:
            break

        if line != '\n':
            ecg_file_writer.write(remove_timestamp(line))


def ecg_conversion(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    bucket_name = file['bucket']
    blob_name = file['name']

    csv_file_reader = get_file_reader(bucket_name, blob_name)
    ecg_file_writer = get_file_writer("innovation-ecg-files", change_file_extenstion(blob_name))

    convert_to_ecg(csv_file_reader, ecg_file_writer)

    csv_file_reader.close()
    ecg_file_writer.flush()
    ecg_file_writer.close()