
### AUTOMATIZACIÓN DE PIPELINES A DATA WAREHOUSE

Proceso de automatización de pipelines a un data warehouse.

<p align="center">
  <img src="https://i.ibb.co/2qMRHBv/etl.png">
</p>


El proceso de automatización realiza el flujo de los datos desde la fuente, en este caso Cloud Storage, hasta el data warehouse, la BigQuery. Utilizando un script de python, hemos creado una función que realiza tareas de procesamiento y carga de datos de manera secuencial.

El script ejecuta las siguientes etapas del proceso de automatización:

1. Configuración: Se establecen las configuraciones necesarias, como los nombres del bucket en Cloud Storage, los identificadores del proyecto, conjunto de datos y tabla en BigQuery.

2. Procesamiento de datos: Se descarga un archivo JSON desde Cloud Storage, se convierte en un DataFrame de pandas y se aplican transformaciones como el cambio de nombres de columnas y eliminación de columnas no deseadas.

3. Preparación de los datos: El DataFrame procesado se guarda temporalmente como un archivo CSV en una ubicación específica.

4. Carga en BigQuery: Se carga el archivo CSV en BigQuery utilizando las configuraciones definidas, permitiendo así almacenar y consultar los datos en una estructura optimizada para análisis.

El proceso de Automatización de pipelines a Data Warehouse se da en un entorno de producción, se ejecuta periódicamente según un cronograma o como respuesta a eventos específicos, para garantizar que los datos se actualicen y estén disponibles en el data warehouse de manera automatizada y confiable.

La automatización del pipelines de datos nos permite ahorrar tiempo y recursos al eliminar la necesidad de realizar estos procesos manualmente. Además, nos brinda la capacidad de mantener un flujo constante de datos hacia el data warehouse, lo que facilita la toma de decisiones basada en información actualizada y precisa.

### VISIÓN GENERAL DE LA ARQUITECTURA

+  **Cloud Scheduler** Es un servicio de Google Cloud que permite programar y automatizar tareas en la nube. En este caso, Cloud Scheduler esta configurado para ejecutar periódicamente la función hello_pubsub() que se muestra en el código. Proporciona la capacidad de disparar eventos en momentos específicos, lo que inicia el procesamiento de datos en el pipeline.

+  **Pub/Sub** Es un servicio de mensajería y publicación-suscripción en Google Cloud. En el código, la función hello_pubsub() está diseñada para ser activada por un mensaje enviado a un topic de Pub/Sub. Cuando se envía un mensaje a ese topic, Pub/Sub lo entrega a la función hello_pubsub(). En el código, el mensaje se decodifica y se imprime, pero también se llama a la función process_data() para iniciar el procesamiento de datos.

+ **Cloud Functions** Es un servicio de Google Cloud que permite ejecutar código de forma serverless en respuesta a eventos. La función hello_pubsub() en el código es una función de Cloud Functions que se activa cuando llega un mensaje al tema de Pub/Sub. La función procesa el mensaje y luego llama a la función process_data() para realizar el procesamiento de datos.

+ **Python Script** El script contiene funciones que procesan y cargan datos en BigQuery utilizando Cloud Storage. Está escrito en Python debido a su facilidad de uso y su amplia disponibilidad de bibliotecas y herramientas para el procesamiento de datos.

+ **BigQuery** Es un servicio de almacenamiento y análisis de datos masivos en la nube de Google. En el código, se utilizan las bibliotecas y funciones de Google Cloud para interactuar con BigQuery. La función process_data() descarga un archivo JSON desde Cloud Storage, lo procesa utilizando pandas y luego carga los datos resultantes en BigQuery.

+ **Cloud Storage** Es un servicio de almacenamiento de objetos en la nube de Google. En el código, se utiliza Cloud Storage para almacenar el archivo JSON que se va a procesar. El script descarga el archivo JSON del bucket especificado y luego realiza las operaciones de procesamiento antes de cargar los datos en BigQuery.