### Extraccion-API-GCS

Con la intenciond de enriquecer los datos utilizados para el proyecto; extraemos archivos provenientes de Yelp y Places API.
<br>
<br>
![DATALAKE (1)](https://github.com/Adapa22/PF-YelpGoogleMaps/blob/main/src/api.PNG)

### Extraccion de las API's

Trabajamos en un archivo *.py* para lograr la extraccion de los datos provenientes de las API's y que nos permita que se suban a un bucket 
en Google Cloud Storage
<br>
<br>
![Recording 2023-06-09 at 09 00 27](https://github.com/Adapa22/PF-YelpGoogleMaps/assets/114951953/a334d179-072a-4ed5-8403-d7e84d4584ca)


### Google Cloud Storage (Data Lake) 
Los datos se recolectan sin procesamiento en un formato JSON y son almacenados junto con la data provista en los datasets de drive;
en un bucket de Google Cloud Storage lo que lo convierte en nuestro *Data Lake*
<br>
<br>


##  Tecnologías 
- Google Cloud Plataform (GCP)
- Cloud Function
- Cloud Storage
- Pandas
- Python

