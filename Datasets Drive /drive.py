import requests
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# Ruta al archivo de credenciales de servicio de Google Cloud Storage
storage_credentials_path = 'credencial GCS'
# Bucket de Google Cloud Storage al que se cargan los datos
bucket_name = "NOMBRE-DEL-BUCKET"

# Crear el cliente de Google Cloud Storage
storage_client = storage.Client.from_service_account_json(storage_credentials_path)

# Ruta al archivo de credenciales de servicio de Google Drive
credenciales = Credencial Drive' 
creden = service_account.Credentials.from_service_account_file(credenciales)
drive_service = build('drive', 'v3', credentials=creden)

# Configurar el bucket destino en google cloud
bucket = storage_client.bucket(bucket_name)

# Lista con los ID de Drive de las carpetas para exportar
lstEstados = [ {'estado' : 'California','googleDriveId':'1Jrbjt-0hnLCvecfrnMwGu1jYZSxElJll'},
                {'estado' : 'New York' ,'googleDriveId':'18HYLDXcKg-cC1CT9vkRUCgea04cNpV33'},
                {'estado' : 'Nevada','googleDriveId':'1W8x6jX1u0fCvpSf0hPHK5rg5jftKEjXK'},
                {'estado' : 'Hawaii','googleDriveId':'1IIAsDSlBiqQEdoN_n0E8a6TTAnrJMyWf'},
                {'estado' : 'Yelp','googleDriveId':'1TI-SsMnZsNP6t930olEEWbBQdo_yuIZF'}
            ]

# Recorre la lista de estados
for itemEstado in lstEstados:
    print(itemEstado.get('estado'))
    print(itemEstado.get('googleDriveId'))

    # Consulta la carpeta en Google Drive
    query = f"'{itemEstado['googleDriveId']}' in parents and trashed = false"
    results = drive_service.files().list(
        q=query,
        includeItemsFromAllDrives=True,
        spaces='drive',
        supportsAllDrives=True,
        fields="nextPageToken, files(id, name)"
    ).execute()

    # Este ciclo recorre los archivos de la carpeta y los sube al bucket de Google Cloud Storage
    print('Subiendo los archivos de la carpeta', itemEstado['estado'])
    for item in results.get('files', []):
        print(itemEstado['estado'] + item['name'])

        # Asigna un nombre "compuesto" para evitar la repetición de archivos
        final_fileName = itemEstado['estado'] + item['name']
        # Obtiene el enlace de descarga del archivo desde Google Drive
        download_link = f"https://drive.google.com/uc?id={item['id']}"
        
        # Configuración de tiempo de espera
        timeout = 60  # Aumenta el tiempo de espera a 60 segundos
        retries = 3  # Número de intentos de conexión

        for i in range(retries):
            try:
                response = requests.get(download_link, timeout=timeout)
                response.raise_for_status()
                break
            except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as err:
                print(f"Error en la descarga del archivo: {err}")
                print(f"Intento {i+1}/{retries}")
                time.sleep(5)  # Pausa de 5 segundos antes de reintentar

        # Sube el archivo directamente desde el enlace a Google Cloud Storage
        blob = bucket.blob(final_fileName)
        blob.upload_from_string(response.content)

print('Trabajo realizado')


