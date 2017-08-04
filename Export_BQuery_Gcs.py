#!/usr/bin/env python

import argparse
import time
import uuid

from google.cloud import bigquery


def export_data_to_gcs(dataset_name, table_name, destination):
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)
    job_name = str(uuid.uuid4())

    job = bigquery_client.extract_table_to_storage(
        job_name, table, destination)

    job.begin()

    wait_for_job(job)

    print('Exported {}:{} to {}'.format(
        dataset_name, table_name, destination))


def wait_for_job(job):
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('dataset_name')
    parser.add_argument('table_name')
    parser.add_argument(
        'destination', help='The desintation Google Cloud Storage object.'
        'Must be in the format gs://bucket_name/object_name')

    args = parser.parse_args()

    export_data_to_gcs(
        args.dataset_name,
        args.table_name,
        args.destination)
