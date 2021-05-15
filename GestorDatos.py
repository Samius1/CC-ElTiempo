from pymongo import MongoClient
from statsmodels.tsa.arima_model import ARIMA
import datetime
import pandas as p
import pickle
import pmdarima as ari
import os
import zipfile as z

class GestorDatos:

   def ObtenerDatos(self, nombreExtra):
      if (nombreExtra == ''):
         return p.read_csv('mezcla.csv', header=0)
      else:
         conexion = MongoClient('mongodb://localhost:4999/')
         baseDatos = conexion.arimaDB
         coleccion = baseDatos.datos
         datosBD = coleccion.find_one({'index': 'prediccion'})
         datos = p.DataFrame(datosBD['data'])
         conexion.close()
         return datos

   def CreacionDeModelos(self, nombreExtra):
      if (not os.path.exists('hum'+nombreExtra+'.zip') or not os.path.exists('temp'+nombreExtra+'.zip')):
         datos = self.ObtenerDatos(nombreExtra)
      
         modeloHumedad = ari.auto_arima(datos['HUM'].dropna(), start_p=1, start_q=1, test='adf', max_p=3, max_q=3, m=1, d=None, seasonal=False, start_P=0, D=0, trace=True, error_action='ignore', suppress_warnings=True, stepwise=True)         
         modeloTemperatura = ari.auto_arima(datos['TEMP'].dropna(), start_p=1, start_q=1, test='adf', max_p=3, max_q=3, m=1, d=None, seasonal=False, start_P=0, D=0, trace=True, error_action='ignore', suppress_warnings=True, stepwise=True)      
         
         pickle.dump(modeloHumedad, open('humedad'+nombreExtra+'.pickle', 'wb'))
         pickle.dump(modeloTemperatura, open('temperatura'+nombreExtra+'.pickle', 'wb'))
      
         humedadZip = z.ZipFile('hum'+nombreExtra+'.zip', 'w', z.ZIP_DEFLATED)
         humedadZip.write('humedad'+nombreExtra+'.pickle')
         humedadZip.close()
      
         temperaturaZip = z.ZipFile('temp'+nombreExtra+'.zip', 'w', z.ZIP_DEFLATED)
         temperaturaZip.write('temperatura'+nombreExtra+'.pickle')
         temperaturaZip.close()
      
      if (not os.path.exists('humedad'+nombreExtra+'.pickle') or not os.path.exists('temperatura'+nombreExtra+'.pickle')):      
         with z.ZipFile('hum'+nombreExtra+'.zip', 'r') as zipObj:
            zipObj.extractall()   
         with z.ZipFile('temp'+nombreExtra+'.zip', 'r') as zipObj:
            zipObj.extractall()   
      
   def ObtenerModelos(self, nombreExtra):
      modelos = []
      
      ficheroHumedad = open('humedad'+nombreExtra+'.pickle', 'rb')
      ficheroTemperatura = open('temperatura'+nombreExtra+'.pickle', 'rb')
      
      modeloHumedadFinal = pickle.load(ficheroHumedad)
      modeloTemperaturaFinal = pickle.load(ficheroTemperatura)   
   
      ficheroHumedad.close()
      ficheroTemperatura.close()
      
      modelos.append(modeloHumedadFinal)
      modelos.append(modeloTemperaturaFinal)
      
      return modelos 
   
   def CrearRangoHoras(self, horas):
      return p.date_range(start=datetime.datetime.now(), periods=horas, freq='H')
      
