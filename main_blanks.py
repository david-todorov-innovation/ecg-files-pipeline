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


def get_timestamp_ecg_pair(line):
    words = line.split(",")
    return int(words[0]), int(words[1])


def get_file_name(line, file_type):
    timestamp = get_timestamp_ecg_pair(line)[0]
    file_name = "".join(["ecg_", str(timestamp), f".{file_type}"])
    return file_name


def calculate_difference(prev_line, curr_line):
    prev_timestamp = get_timestamp_ecg_pair(prev_line)[0]
    curr_timestamp = get_timestamp_ecg_pair(curr_line)[0]
    return curr_timestamp - prev_timestamp


def how_many_missing_timestamps(prev_line, curr_line):
    diff = calculate_difference(prev_line, curr_line)
    if diff % 8 != 0:
        raise Exception("The difference between timestamps is not dividable by 8.")
    return (diff / 8) - 1


def are_there_missing_timestamps(prev_line, curr_line):
    return how_many_missing_timestamps(prev_line, curr_line) > 0


def write_missing_timestamps(prev_line, curr_line, dest_file_writer):
    prev_timestamp = get_timestamp_ecg_pair(prev_line)[0]
    curr_timestamp = get_timestamp_ecg_pair(curr_line)[0]

    for to_add in range(prev_timestamp + 8, curr_timestamp, 8):
        dest_file_writer.write("".join([str(to_add), ",-1\n"]))


def check_and_fill_in_blanks(src_file_bucket, src_file_name, csv_dest_bucket):
    src_file_reader = get_file_reader(src_file_bucket, src_file_name)
    prev_line = src_file_reader.readline()
    temp_csv_file_name = get_file_name(prev_line, "csv")
    dest_file_writer = get_file_writer(csv_dest_bucket, temp_csv_file_name)
    while True:
        # Get next line from file
        curr_line = src_file_reader.readline()

        # if line is empty, end of file is reached
        if not curr_line:
            break

        dest_file_writer.write(prev_line)
        if are_there_missing_timestamps(prev_line, curr_line):
            if calculate_difference(prev_line, curr_line) < 30000:
                write_missing_timestamps(prev_line, curr_line, dest_file_writer)
            else:
                dest_file_writer.flush()
                dest_file_writer.close()
                prev_line = curr_line
                temp_csv_file_name = get_file_name(prev_line, "csv")
                dest_file_writer = get_file_writer(csv_dest_bucket, temp_csv_file_name)
                continue

        prev_line = curr_line

    dest_file_writer.flush()
    dest_file_writer.close()


def fill_in_blanks(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    bucket_name = file['bucket']
    blob_name = file['name']

    delete_all_blobs("blanks-filled-in-csv-file")

    if blob_name == "merged.csv":
        print(f"Processing file: {file['name']}.")
        check_and_fill_in_blanks(bucket_name, blob_name, "blanks-filled-in-csv-file")
