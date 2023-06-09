import requests
from google.cloud import storage
import json
import datetime

#Credenciales de GCS
storage_credentials_path = 'credenciales de GCS'

#Nombre del bucket
bucket_name = 'nombre del bucket'

# Crear el cliente de Google Cloud Storage
storage_client = storage.Client.from_service_account_json(storage_credentials_path)

# Función para cargar datos en el Data Lake
def upload_data_to_bucket(data, filename):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    # Cargar los datos en el Blob
    blob.upload_from_string(data)

def obtener_resenas_estado(estados_ciudades, google_api_key):
    reviews_data = []
    
    for estado, ciudades in estados_ciudades.items():
        for ciudad in ciudades:
            query = f"restaurant+in+{ciudad}+{estado}"
            url = f'https://maps.googleapis.com/maps/api/place/textsearch/json?key={google_api_key}&query={query}'
            response = requests.get(url)
            data = response.json()
            
            if 'results' in data and len(data['results']) > 0:
                place_ids = [result['place_id'] for result in data['results']]
                
                for place_id in place_ids:
                    url = f'https://maps.googleapis.com/maps/api/place/details/json?key={google_api_key}&place_id={place_id}&fields=name,formatted_address,reviews,types,price_level,user_ratings_total'
                    response = requests.get(url)
                    data = response.json()
                    
                    if 'result' in data:
                        restaurant_name = data['result'].get('name')
                        restaurant_location = data['result'].get('formatted_address')
                        reviews = data['result'].get('reviews', [])
                        category = data['result'].get('types', [])  # Agregar el campo 'categoria'
                        price_level = data['result'].get('price_level')  # Agregar el campo 'price_level'
                        user_ratings_total = data['result'].get('user_ratings_total')  # Agregar el campo 'user_ratings_total'
                        
                        for review in reviews:
                            review['restaurant_name'] = restaurant_name
                            review['restaurant_location'] = restaurant_location
                            review['gmap_id'] = place_id  # Agregar el campo 'gmap_id'
                            review['category'] = category  # Agregar el campo 'categoria'
                            review['price_level'] = price_level  # Agregar el campo 'costoso'
                            review['user_ratings_total'] = user_ratings_total  # Agregar el campo 'user_ratings_total'
                            reviews_data.append(review)
    
    return json.dumps(reviews_data)

# Obtener datos de la API de Google
google_api_key = "AIzaSyDlTHO19VPc5BZF5Z2ZC5mJQvdWZTEzdKg"
estados_ciudades = {
    'New York': ['New York City', 'Buffalo', 'Rochester'],
    'California': ['Los Angeles', 'San Francisco', 'San Diego'],
    'Florida': ['Miami', 'Orlando', 'Tampa'],
    'Nevada': ['Las Vegas', 'Reno'],
    'Hawaii': ['Honolulu', 'Maui', 'Kauai']
}

# Obtener las reseñas por estado y ciudades turísticas
reviews_data = obtener_resenas_estado(estados_ciudades, google_api_key)

# Verificar si se obtuvieron reseñas
if reviews_data:
    # Generar el nombre de archivo con la fecha actual
    fecha_actual = datetime.datetime.now().strftime("%Y%m%d")
    filename = f"reviews_{fecha_actual}.json"

    # Subir los datos al bucket de Google Cloud Storage
    upload_data_to_bucket(reviews_data, filename)
    print(f"Datos de reseñas cargados en el bucket: {bucket_name}, archivo: {filename}")
else:
    print(f"No se encontraron reseñas para los estados: {estados_ciudades.keys()}")
