import os
import json
import requests
import csv
from google.cloud import storage
from google.cloud import bigquery
import functions_framework

# Configuration
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "etlpipelinehomeworkhbcloudrun")
BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID", "coursera_data")
BQ_TABLE_ID = os.environ.get("BQ_TABLE_ID", "courses")

# GraphQL API endpoint and headers
URL = "https://www.coursera.org/graphql-gateway?opname=DiscoveryCollections"
HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "operation-name": "DiscoveryCollections",
    "origin": "https://www.coursera.org",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "x-csrf3-token": "1743681871.xQuQa8u8tAbHg2hP",  # Replace with the fresh token
}
PAYLOAD = [
    {
        "operationName": "DiscoveryCollections",
        "variables": {"contextType": "PAGE", "contextId": "search-zero-state"},
        "query": """
        query DiscoveryCollections($contextType: String!, $contextId: String!, $passThroughParameters: [DiscoveryCollections_PassThroughParameter!]) {
          DiscoveryCollections {
            queryCollections(
              input: {contextType: $contextType, contextId: $contextId, passThroughParameters: $passThroughParameters}
            ) {
              id
              label
              entities {
                id
                slug
                name
                url
              }
            }
          }
        }
    """,
    }
]

# BigQuery schema
SCHEMA = [
    bigquery.SchemaField("collection_label", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("collection_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("course_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("course_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("slug", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
]

CSV_HEADERS = [field.name for field in SCHEMA]


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"Uploaded {source_file_name} to gs://{bucket_name}/{destination_blob_name}")


@functions_framework.http
def main(request):
    try:
        response = requests.post(URL, headers=HEADERS, json=PAYLOAD)
        if response.status_code != 200:
            print(f"API failed with status {response.status_code}: {response.text}")
            raise Exception(f"API request failed: {response.status_code}")
        data = response.json()
        print("API request succeeded")

        json_file_path = "/tmp/coursera_response.json"
        with open(json_file_path, "w") as json_file:
            json.dump(data, json_file)
        upload_to_gcs(GCS_BUCKET_NAME, json_file_path, "coursera_response.json")

        collections = data[0]["data"]["DiscoveryCollections"]["queryCollections"]
        transformed_data = []
        for collection in collections:
            collection_label = collection["label"]
            collection_id = collection["id"]
            for entity in collection["entities"]:
                row = {
                    "collection_label": collection_label,
                    "collection_id": collection_id,
                    "course_name": entity["name"],
                    "course_id": entity["id"],
                    "slug": entity["slug"],
                    "url": entity["url"],
                }
                transformed_data.append(row)

        csv_file_path = "/tmp/coursera_courses.csv"
        with open(csv_file_path, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=CSV_HEADERS)
            writer.writeheader()
            writer.writerows(transformed_data)
        upload_to_gcs(GCS_BUCKET_NAME, csv_file_path, "coursera_courses.csv")

        bq_client = bigquery.Client()
        dataset_ref = bq_client.dataset(BQ_DATASET_ID)
        try:
            bq_client.get_dataset(dataset_ref)
        except:
            bq_client.create_dataset(dataset_ref)
        table_ref = dataset_ref.table(BQ_TABLE_ID)
        try:
            bq_client.delete_table(table_ref)
        except:
            pass
        table = bigquery.Table(table_ref, schema=SCHEMA)
        bq_client.create_table(table)
        errors = bq_client.insert_rows_json(table, transformed_data)
        if errors:
            raise Exception(f"BigQuery insert errors: {errors}")

        return (
            json.dumps({"status": "success"}),
            200,
            {"Content-Type": "application/json"},
        )

    except Exception as e:
        print(f"Error: {str(e)}")
        return (
            json.dumps({"status": "error", "message": str(e)}),
            500,
            {"Content-Type": "application/json"},
        )
