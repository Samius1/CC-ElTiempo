from pymongo import MongoClient
from statsmodels.tsa.arima_model import ARIMA
import datetime
import flask
import json
import os
import pandas as p
import pickle
import pmdarima as ari
import zipfile as z

app = flask.Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
   return "<h1>API utilizando ARIMA con BD</h1><p>API de ejemplo para obtener la predicción del tiempo utilizando ARIMA utilizando conexión con base de datos.</p>"

@app.route('/v3/<int:horas>', methods=['GET'])
def arimaBD(horas):
   if (horas <= 0 or horas > 72):
      return flask.Response('Número incorrecto. Introduzca un número mayor que 0 y menor que 73.', status=400)
   
   if (not os.path.exists('hum_db.zip') or not os.path.exists('temp_db.zip')):
      conexion = MongoClient('mongodb://localhost:4999/')
      baseDatos = conexion.arimaDB
      coleccion = baseDatos.datos
      datosBD = coleccion.find_one({'index': 'prediccion'})
      datos = p.DataFrame(datosBD['data'])
      conexion.close()
      
      modeloHumedad = ari.auto_arima(datos['HUM'].dropna(), start_p=1, start_q=1, test='adf', max_p=3, max_q=3, m=1, d=None, seasonal=False, start_P=0, D=0, trace=True, error_action='ignore', suppress_warnings=True, stepwise=True)         
      modeloTemperatura = ari.auto_arima(datos['TEMP'].dropna(), start_p=1, start_q=1, test='adf', max_p=3, max_q=3, m=1, d=None, seasonal=False, start_P=0, D=0, trace=True, error_action='ignore', suppress_warnings=True, stepwise=True)      
         
      pickle.dump(modeloHumedad, open('humedad_db.pickle', 'wb'))
      pickle.dump(modeloTemperatura, open('temperatura_db.pickle', 'wb'))
      
      humedadZip = z.ZipFile('hum_db.zip', 'w', z.ZIP_DEFLATED)
      humedadZip.write('humedad_db.pickle')
      humedadZip.close()
      
      temperaturaZip = z.ZipFile('temp_db.zip', 'w', z.ZIP_DEFLATED)
      temperaturaZip.write('temperatura_db.pickle')
      temperaturaZip.close()
      
   if (not os.path.exists('humedad_db.pickle') or not os.path.exists('temperatura_db.pickle')):      
      with z.ZipFile('hum_db.zip', 'r') as zipObj:
         zipObj.extractall()   
      with z.ZipFile('temp_db.zip', 'r') as zipObj:
         zipObj.extractall()   
      
   ficheroHumedad = open('humedad_db.pickle', 'rb')
   ficheroTemperatura = open('temperatura_db.pickle', 'rb')
      
   modeloHumedadFinal = pickle.load(ficheroHumedad)
   modeloTemperaturaFinal = pickle.load(ficheroTemperatura)   
   
   ficheroHumedad.close()
   ficheroTemperatura.close()
         
   prediccionHumedad, conf = modeloHumedadFinal.predict(horas, return_conf_int=True)
   prediccionTemperatura, confint = modeloTemperaturaFinal.predict(horas, return_conf_int=True)   
   rango_horas = p.date_range(start=datetime.datetime.now(), periods=horas + 1, freq='H')
   
   resultadoArima = []
   
   for hora, temperatura, humedad in zip(rango_horas, prediccionTemperatura, prediccionHumedad):
      resultadoArima.append( { "hour": hora.strftime('%H:00'), "temp": round(temperatura,2), "hum": round(humedad,2) })
   
   if (len(resultadoArima) == 0):
      return flask.Response("No se ha encontrado información.", status=400)
   else:
      return flask.Response(json.dumps(resultadoArima), mimetype='application/json', status=200)

