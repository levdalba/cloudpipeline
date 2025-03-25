import os
import json
import requests
import csv
from google.cloud import storage
from google.cloud import bigquery
import functions_framework

# Configuration via environment variables (set in Cloud Run deployment)
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "gcstestbuckethb")
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
    "x-csrf3-token": "1743681871.xQuQa8u8tAbHg2hP",  # Update this token as needed
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
              id label entities { id slug name url partnerIds imageUrl partners { name }
                difficultyLevel isPartOfCourseraPlus courseCount isCostFree
                productCard { marketingProductType productTypeAttributes { isPathwayContent } }
              }
            }
          }
        }
    """,
    }
]

# BigQuery schema definition
SCHEMA = [
    bigquery.SchemaField("collection_label", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("collection_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("course_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("course_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("slug", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("image_url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("partners", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("partner_ids", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("difficulty_level", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("is_part_of_coursera_plus", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("course_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("is_cost_free", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("marketing_product_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("is_pathway_content", "BOOLEAN", mode="NULLABLE"),
]

# CSV headers for transformed data
CSV_HEADERS = [field.name for field in SCHEMA]


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"Uploaded {source_file_name} to gs://{bucket_name}/{destination_blob_name}")


@functions_framework.http
def main(request):
    """HTTP Cloud Function that runs the ETL pipeline."""
    try:
        # 1. Extract data from GraphQL API
        response = requests.post(URL, headers=HEADERS, json=PAYLOAD)
        if response.status_code != 200:
            raise Exception(f"API request failed: {response.status_code}")
        data = response.json()

        # 2. Save raw JSON to GCS
        json_file_path = "/tmp/coursera_response.json"
        with open(json_file_path, "w") as json_file:
            json.dump(data, json_file)
        upload_to_gcs(GCS_BUCKET_NAME, json_file_path, "coursera_response.json")

        # 3. Transform data
        collections = data[0]["data"]["DiscoveryCollections"]["queryCollections"]
        transformed_data = []
        for collection in collections:
            collection_label = collection["label"]
            collection_id = collection["id"]
            for entity in collection["entities"]:
                partners = ", ".join(
                    [partner["name"] for partner in entity["partners"]]
                )
                partner_ids = ", ".join(entity["partnerIds"])
                row = {
                    "collection_label": collection_label,
                    "collection_id": collection_id,
                    "course_name": entity["name"],
                    "course_id": entity["id"],
                    "slug": entity["slug"],
                    "url": entity["url"],
                    "image_url": entity["imageUrl"],
                    "partners": partners,
                    "partner_ids": partner_ids,
                    "difficulty_level": entity.get("difficultyLevel"),
                    "is_part_of_coursera_plus": entity.get("isPartOfCourseraPlus"),
                    "course_count": entity.get("courseCount"),
                    "is_cost_free": entity.get("isCostFree"),
                    "marketing_product_type": entity.get("productCard", {}).get(
                        "marketingProductType"
                    ),
                    "is_pathway_content": entity.get("productCard", {})
                    .get("productTypeAttributes", {})
                    .get("isPathwayContent"),
                }
                transformed_data.append(row)

        # 4. Save transformed data to CSV and upload to GCS
        csv_file_path = "/tmp/coursera_courses.csv"
        with open(csv_file_path, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=CSV_HEADERS)
            writer.writeheader()
            writer.writerows(transformed_data)
        upload_to_gcs(GCS_BUCKET_NAME, csv_file_path, "coursera_courses.csv")

        # 5. Load transformed data into BigQuery
        bq_client = bigquery.Client()
        dataset_ref = bq_client.dataset(BQ_DATASET_ID)

        # Create dataset if it doesn't exist
        try:
            bq_client.get_dataset(dataset_ref)
        except:
            bq_client.create_dataset(dataset_ref)

        table_ref = dataset_ref.table(BQ_TABLE_ID)
        # Delete and recreate table for a fresh load
        try:
            bq_client.delete_table(table_ref)
        except:
            pass  # Table doesn't exist
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
