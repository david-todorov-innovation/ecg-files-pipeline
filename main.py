from google.cloud import storage
from heapq import merge
import time


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


def pairwise(iterable):
    """s -> (s0, s1), (s2, s3), (s4, s5), ..."""
    a = iter(iterable)
    return zip(a, a)


def calculate_y(x):
    y = (x / 6) + 511
    if y > 1023:
        y = 1023
    elif y < 0:
        y = 0
    return int(y)


def write_to_bucket(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # getting information for the file name and the bucket name that triggered the function
    file = event
    bucket_name = event['bucket']
    blob_name = event['name']

    print(f"Processing file: {file['name']}.")

    # creating a read stream for the file that triggered the function
    input_reader = get_file_reader(bucket_name, file['name'])

    # reading the file line by line, splitting the lines in pairs and writing each
    # timestamp,x pair in a list of input file lines.
    inputFileLines = []
    while True:
        # Get next line from file
        line = input_reader.readline()

        # if line is empty, end of file is reached
        if not line:
            break

        words = line.split(",")
        for timestamp, x in pairwise(words):
            if timestamp and x:
                row = ",".join([timestamp, str(calculate_y(int(x)))])
                if row.endswith("\n"):
                    inputFileLines.append(row)
                    # writer.write(row)
                else:
                    inputFileLines.append(row + "\n")
                    # writer.write(row + "\n")
            else:
                break

    input_reader.close()
    inputFileLines.sort()

    # creating a file writing stream for a temporary file where the content will be written
    writer = get_file_writer("merged-formatted-csv-file", "merged-tmp.csv")

    # if a merged file already exists, it's merge-sorted with the input file
    start = time.time()
    if blob_exists("merged-formatted-csv-file", "merged.csv"):
        # read stream for reading from the merged file
        merged_reader = get_file_reader("merged-formatted-csv-file", "merged.csv")

        line = merged_reader.readline()

        while True:
            # End of file has been reached
            if not line:
                # The merged file has been read and written in the merged-tmp file in its entirety.
                # The rest of the input file lines list should be written to the merged-tmp file.
                for row in inputFileLines:
                    writer.write(row)
                break

            # End of list has been reached
            if not inputFileLines:
                # The input file list has been written in the merged-tmp file in its entirey.
                # The rest of the merged file should be written to the merged-tmp file.
                writer.write(line)
                while True:
                    line = merged_reader.readline()
                    if not line:
                        break
                    writer.write(line)
                break

            # comparing lines from the file and the list. The smaller line is written to
            # the output file, and "the pointer" is moved to the next line in the resource.
            if line < inputFileLines[0]:
                writer.write(line)
                line = merged_reader.readline()
            else:
                writer.write(inputFileLines.pop(0))

        merged_reader.close()
    else:
        # This is the first file that has been sent, so it will
        # need to be sorted and written to the output bucket
        for row in inputFileLines:
            writer.write(row)

    end = time.time()
    print(f"Sorting took {end - start} seconds")

    writer.flush()
    writer.close()

    # If a merged file already existed, its contents are now in
    # the temporary file, so the merged file is safe to delete.
    if blob_exists("merged-formatted-csv-file", "merged.csv"):
        delete_blob("merged-formatted-csv-file", "merged.csv")

    # the temporary file is renamed to be the main merged file
    rename_blob("merged-formatted-csv-file", "merged-tmp.csv", "merged.csv")
