from google.cloud import storage, bigquery
import pandas as pd
import base64
import csv
from textblob import TextBlob

def process_data(event, context):
    # Configuración de Cloud Storage
    bucket_name = 'bucket_datacket'
    file_name = 'reviews_gm_all.json'

    # Configuración de BigQuery
    project_id = 'proyectogrupal'
    dataset_id = 'dataset_gm'
    table_id = 'tb_gm'

    # Configuración de columnas (renombrar y eliminar)
    column_mapping = {
        'author_name': 'user_name',
        'text': 'reviews',
        'time': 'date'
    }
    columns_to_drop = ['author_url', 'profile_photo_url', 'language', 'original_language', 'profile_photo_url', 'relative_time_description', 'translated']

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

    # Eliminar filas con valores nulos
    df = df.dropna()

    # Combinar múltiples categorías en una sola cadena
    df['category'] = df['category'].apply(lambda x: ' '.join(x))

    # Eliminar duplicados
    df = df.drop_duplicates()
    
    # Reemplazar los valores nulos de 'price_level' con la mediana calculada de esa misma columna
    median_price_level = df['price_level'].median()
    df['price_level'].fillna(median_price_level, inplace=True)
    
    # Validar el sentimiento del texto en la columna 'reviews' y asignar etiquetas
    df['sentiment'] = df['reviews'].apply(lambda x: 'positive' if TextBlob(x).sentiment.polarity > 0 else 'negative' if TextBlob(x).sentiment.polarity < 0 else 'neutral')

    # Convertir el formato de fecha timestamp a fecha normal
    df['date'] = pd.to_datetime(df['date'], unit='s')
    
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

