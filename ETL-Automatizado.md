
## AUTOMATIZACIÓN DE PIPELINES ETL A DATA WAREHOUSE

<p align="center">
  <img src="https://github.com/Adapa22/PF-YelpGoogleMaps/blob/main/src/etl3.PNG">
</p>

<!-- TABLA DE CONTENIDO -->
## Indice
<details open="open">
  <summary>Tabla de contenido: </summary>
  <ol>
    <li>
      <a href="#Proceso-de-ETL-Automatizado">Proceso de ETL Automatizado</a>
      <ul>
        <li><a href="#Data-Lake">Data Lake</a></li>
        <li><a href="#Pub-Sub">Pub Sub</a></li>
        <li><a href="#Cloud-Functions">Cloud Functions</a></li>
        <li><a href="#Script">Script</a></li>
        <li><a href="#Cloud-Scheduler">Cloud Scheduler</a></li>
        <li><a href="#Data-Warehouse">Data Warehouse</a></li>
      </ul> 
    </li>
    <li>
      <a href="#Vision-General">Visión General</a>
    </li>
    <li>
      <a href="#Stack-Tecnológico">Stack Tecnológico</a>
    </li>
  </ol>
</details>

## Data Lake
Disponibilizamos nuestro Data Lake, el mismo que se utiliza para describir la arquitectura de almacenamiento de datos en formato json que permite almacenar grandes volúmenes de datos en su formato original, sin estructuración previa. En nuestro Data Lake, los datos fueron almacenados luego del proceso de pipelines de extracción automatizada antes de aplicar las transformaciones en el proceso de ETL.

Nuestro Data Lake proporciona una capa de almacenamiento flexible y escalable para los datos antes de ser estructurados y analizados en BigQuery, actuando como una fuente centralizada y versátil para los datos en bruto.
<p align="center">
  <img src="https://github.com/Adapa22/PF-YelpGoogleMaps/blob/main/src/cs2.PNG">
</p>

## Pub Sub
Pub/Sub es un servicio de mensajería y publicación en la nube. En nuestro proceso de ETL automatizado, se utiliza como un medio de comunicación entre los diferentes componentes del flujo de trabajo. Cuando se envía un mensaje a un tema de Pub/Sub, se desencadena la ejecución de la función hello_pubsub, que a su vez inicia el proceso de ETL.
<p align="center">
  <img src="https://github.com/Adapa22/PF-YelpGoogleMaps/blob/main/src/psub.PNG">
</p>

## Script
El script contiene funciones que procesan y cargan datos a BigQuery desde Cloud Storage. Está escrito en Python debido a su facilidad de uso y su amplia disponibilidad de bibliotecas y herramientas para el procesamiento de datos.

## Cloud Functions
Cloud Functions es un servicio de computación sin servidor que nos permite ejecutar código en respuesta a eventos. En nuestro proceso de pipelines, la función hello_pubsub se configura como una Cloud Function, que se activa cuando se recibe un mensaje en el tema de Pub/Sub. Esta función decodifica el mensaje en base64, imprime su contenido y luego llama a la función process_data.
<p align="center">
  <img src="https://github.com/Adapa22/PF-YelpGoogleMaps/blob/main/src/cfunctions.PNG">
</p>

<p align="center">
  <img src="https://github.com/Adapa22/PF-YelpGoogleMaps/blob/main/src/cf.gif">
</p>
<p align="center">
  <img src="https://github.com/Adapa22/PF-YelpGoogleMaps/blob/main/src/cf2.gif">
</p>

## Cloud Scheduler
Cloud Scheduler es un servicio que nos permite programar la ejecución de tareas en la nube. Configuramos Cloud Scheduler para que publique un mensaje en el tema de Pub/Sub en intervalos regulares, lo que desencadenará la ejecución del flujo de trabajo de ETL. En nuestro proceso de ETL se programó para ejecutarse automáticamente todos los lunes a las 9 AM. 
<p align="center">
  <img src="https://github.com/Adapa22/PF-YelpGoogleMaps/blob/main/src/cscheduler2.PNG">
</p>

<p align="center">
  <img src="https://github.com/Adapa22/PF-YelpGoogleMaps/blob/main/src/csch.gif">
</p>

## Data Warehouse
Después de realizar las transformaciones y limpieza de los datos, el DataFrame de pandas lo guarda como un archivo CSV temporal y se carga en una tabla de BigQuery.Este actúa como un almacén de datos, también conocido como data warehouse.

En nuestro Data Warehouse almacenamos los datos estructurados de manera eficiente para su posterior análisis y consulta.

<p align="center">
  <img src="https://github.com/Adapa22/PF-YelpGoogleMaps/blob/main/src/bq.gif">
</p>

## Visión General
El proceso de Automatización de pipelines a Data Warehouse se da en un entorno de producción, se ejecuta periódicamente según un cronograma o como respuesta a eventos específicos, para garantizar que los datos se actualicen y estén disponibles en el data warehouse de manera automatizada y confiable.

La automatización del pipelines de datos nos permite ahorrar tiempo y recursos al eliminar la necesidad de realizar estos procesos manualmente. Además, nos brinda la capacidad de mantener un flujo constante de datos hacia el Data Warehouse, lo que facilita la toma de decisiones basada en información actualizada y precisa.

## Stack Tecnológico
+ Cloud Storage
+ Pub/Sub
+ Cloud Functions
+ Cloud Scheduler
+ BigQuery



