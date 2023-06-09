from google.cloud import storage, bigquery
import pandas as pd
import base64
import csv

def process_data(event, context):
    # Configuración de Cloud Storage
    bucket_name = 'bucket_name'
    file_name = 'file_name'

    # Configuración de BigQuery
    project_id = 'fine-sublime-388323'
    dataset_id = 'etl_prueba'
    table_id = 'tb_etl'

    # Configuración de columnas (renombrar y eliminar)
    column_mapping = {
        'author_name': 'user_name',
        'text': 'reviews'
    }
    columns_to_drop = ['author_url', 'profile_photo_url']

    # Inicializar clientes de Cloud Storage y BigQuery
    storage_client = storage.Client()
    bq_client = bigquery.Client(project=project_id)

    # Descargar el archivo JSON desde Cloud Storage
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    json_data = blob.download_as_text()

    # Convertir el JSON a un DataFrame de pandas
    df = pd.read_json(json_data)

    # Renombrar columnas
    df = df.rename(columns=column_mapping)

    # Eliminar columnas
    df = df.drop(columns=columns_to_drop)

    # Guardar el DataFrame como un archivo CSV temporal
    temp_csv_file = '/tmp/temp_data.csv'
    df.to_csv(temp_csv_file, index=False, quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')

    # Cargar el archivo CSV en BigQuery
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig(
        autodetect=True, 
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        quote_character='"',
        allow_quoted_newlines=True
    )

    with open(temp_csv_file, "rb") as source_file:
        job = bq_client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Esperar a que el proceso de carga se complete

    print("Proceso completado con éxito.")

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    process_data(event, context)

if __name__ == "__main__":
    event = {
        'data': 'data'
    }
    context = 'context'
    hello_pubsub(event, context)
