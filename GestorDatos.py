import pandas as p
import pymongo
import os

def GuardarDatos(directorio):
   humedad = p.read_csv(directorio + 'humidity.csv')
   temperatura = p.read_csv(directorio + 'temperature.csv')
   
   columnaHumedad = humedad['San Francisco']
   columnaTemperatura = temperatura['San Francisco']
   columnaFecha = temperatura['datetime']
   
   tablaDatos = { 
      'DATE': columnaFecha, 
      'HUM': columnaHumedad, 
      'TEMP': columnaTemperatura 
   } 
   
   frame = pandas.DataFrame(data = tablaDatos)
   
   frame.to_csv(directorio + 'mezcla.csv')
      
