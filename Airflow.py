from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pymongo import MongoClient

import pandas as p
import os

def GuardarDatos():
   humedad = p.read_csv('~/Desktop/SPA_Airflow/humidity.csv')
   temperatura = p.read_csv('~/Desktop/SPA_Airflow/temperature.csv')
   
   columnaHumedad = humedad['San Francisco']
   columnaTemperatura = temperatura['San Francisco']
   columnaFecha = temperatura['datetime']
   
   tablaDatos = { 
      'DATE': columnaFecha, 
      'HUM': columnaHumedad, 
      'TEMP': columnaTemperatura 
   } 
   
   frame = p.DataFrame(data = tablaDatos)
   
   frame.to_csv('~/Desktop/SPA_Airflow/ElTiempo/mezcla.csv')
   
   conexion = MongoClient('mongodb://localhost:4999/')
   baseDatos = conexion.arimaDB
   coleccion = baseDatos.datos
   diccionario = frame.to_dict("records")
   coleccion.insert_one({'index':'prediccion', 'data': diccionario})
   conexion.close()
   

argumentos = {
   'depends_on_past': False,
   'email': ['samuel11.1.93@gmail.com', 'samu11193@correo.ugr.es'],
   'email_on_failure': False,
   'email_on_retry': False,
   'owner': 'airflow',
   'retries': '2',
   'retry_delay': timedelta(minutes=5)
}

with DAG(
   'PracticaAirflow',
   default_args = argumentos,
   description = 'Flujo de trabajo para la práctica de Airflow',
   start_date = days_ago(2),
   schedule_interval = timedelta(days=1)
) as dag:

   InicializarSistema = BashOperator(
      bash_command = 'mkdir -p ~/Desktop/SPA_Airflow; docker run -d -p 4999:27017 --name mongodb mongo:4.0;',
      dag = dag,
      depends_on_past = False,
      task_id = 'InicializarSistema'
   )

   DescargarDatosHumedad = BashOperator(
      bash_command = 'wget https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/humidity.csv.zip --output-document ~/Desktop/SPA_Airflow/humedad.zip',
      dag = dag,
      depends_on_past = False,
      task_id = 'DescargarDatosHumedad'
   )

   DescargarDatosTemperatura = BashOperator(
      bash_command = 'wget https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/temperature.csv.zip --output-document ~/Desktop/SPA_Airflow/temperatura.zip',
      dag = dag,
      depends_on_past = False,
      task_id = 'DescargarDatosTemperatura'
   )
   
   DescomprimirDatosTiempo = BashOperator(
      bash_command = 'unzip -d ~/Desktop/SPA_Airflow -o ~/Desktop/SPA_Airflow/temperatura.zip; unzip -d ~/Desktop/SPA_Airflow -o ~/Desktop/SPA_Airflow/humedad.zip;',
      dag = dag,
      depends_on_past = False,
      task_id = 'DescomprimirDatosTiempo'
   )

   DescargarProyectoElTiempo = BashOperator(
      bash_command = 'rm -rf ~/Desktop/SPA_Airflow/ElTiempo; mkdir -p ~/Desktop/SPA_Airflow/ElTiempo; git clone https://github.com/Samius1/CC-ElTiempo.git ~/Desktop/SPA_Airflow/ElTiempo;',
      dag = dag,
      depends_on_past = False,
      task_id = 'DescargarProyectoElTiempo'
   )

   GuardarDatosTiempo = PythonOperator(
      dag = dag,
      op_kwargs = {},
      python_callable = GuardarDatos,
      task_id = 'GuardarDatosTiempo'
   )
   
   TestValidos = BashOperator(
      bash_command = 'cd ~/Desktop/SPA_Airflow/ElTiempo; python3 -m unittest tests.py;',
      dag = dag,
      depends_on_past = False,
      task_id = 'TestValidos'
   )
   
   API_Arima = BashOperator(
      bash_command = 'cd ~/Desktop/SPA_Airflow/ElTiempo; docker build -f DockerfileArima -t api1_arima .; docker run -d -p 5001:5001 api1_arima;',
      dag = dag,
      depends_on_past = False,
      task_id = 'API_Arima'
   )
   
   API_OpenWeather = BashOperator(
      bash_command = 'cd ~/Desktop/SPA_Airflow/ElTiempo; docker build -f DockerfileOpenWeather -t api2_openweather .; docker run -d -p 5002:5002 api2_openweather;',
      dag = dag,
      depends_on_past = False,
      task_id = 'API_OpenWeather'
   )
   
   API_Arima_DB = BashOperator(
      bash_command = 'cd ~/Desktop/SPA_Airflow/ElTiempo; docker build -f DockerfileArimaDB -t api1_arima_db .; docker run -d -p 5003:5003 api1_arima_db;',
      dag = dag,
      depends_on_past = False,
      task_id = 'API_Arima_DB'
   )

   # Flujo del proceso
   InicializarSistema >> [DescargarDatosHumedad, DescargarDatosTemperatura, DescargarProyectoElTiempo] >> DescomprimirDatosTiempo >> GuardarDatosTiempo >> TestValidos >> [API_Arima, API_OpenWeather, API_Arima_DB]

