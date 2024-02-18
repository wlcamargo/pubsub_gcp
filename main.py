import base64
from google.cloud import bigquery
from datetime import datetime
import functions_framework

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def sales_ecommerce(cloud_event):
    # Decode a base64-encoded message from Pub/Sub
    decoded_message = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")

    # Timestamp insert
    insert_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Configs BigQuery
    project_id = "sonic-airfoil-411711"
    dataset_id = "dev"
    table_id = "mensagem"

    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    # Create table BigQuery if not exists
    try:
        client.get_table(table_ref)
    except Exception as e:
        schema = [
            bigquery.SchemaField("insert_date", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("mensagem_real_time", "STRING", mode="REQUIRED")
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Tabela BigQuery {project_id}.{dataset_id}.{table_id} criada.")

    # Insert data BigQuery
    row = {
        "insert_date": insert_date,
        "mensagem_real_time": decoded_message,
    }

    errors = client.insert_rows_json(table_ref, [row])

    if errors:
        print(f"Erro ao inserir linha no BigQuery: {errors}")
    else:
        print(f"Linha inserida no BigQuery: {row}")