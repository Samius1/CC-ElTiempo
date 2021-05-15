import api1_arima
import api1_arima_db
import api2_openweather
import GestorDatos
import json
import pandas
import unittest
import os


gestorDatos = GestorDatos.GestorDatos()
api1 = api1_arima.app.test_client()
api2 = api2_openweather.app.test_client()
api1BD = api1_arima_db.app.test_client()

class TestGestorDatos(unittest.TestCase):

   def test_CreacionDeModelos_ElModeloExiste(self):      
      gestorDatos.CreacionDeModelos('')
      assert True == os.path.isfile('./hum.zip')
      assert True == os.path.isfile('./temp.zip')

   def test_CreacionDeModelosBD_ElModeloExiste(self):      
      gestorDatos.CreacionDeModelos('_db')
      assert True == os.path.isfile('./hum_db.zip')
      assert True == os.path.isfile('./temp_db.zip')
      
   def test_ObtenerModelos_DevuelveDosModelos(self):
      modelos = gestorDatos.ObtenerModelos('')
      assert 2 == len(modelos)
      
   def test_ObtenerModelos_DevuelveUnaLista(self):
      modelos = gestorDatos.ObtenerModelos('')
      assert list == type(modelos)
      
   def test_ObtenerModelosDB_DevuelveDosModelos(self):
      modelos = gestorDatos.ObtenerModelos('_db')
      assert 2 == len(modelos)
   
   def test_ObtenerModelosDB_DevuelveUnaLista(self):
      modelos = gestorDatos.ObtenerModelos('_db')
      assert list == type(modelos)
      
   def test_CrearRangoHoras_DevuelveUnaLista(self):
      rangoHoras = gestorDatos.CrearRangoHoras(1)
      assert pandas.core.indexes.datetimes.DatetimeIndex == type(rangoHoras)
   
   def test_CrearRangoHoras_SeLlamaConUnPeriodoDeCinco_DevuelveCincoElementos(self):
      rangoHoras = gestorDatos.CrearRangoHoras(5)
      assert 5 == rangoHoras.value_counts().count()
      
   def test_API1Arima_PidoCincoHoras_DevuelveListaDeCincoElementos(self):
      peticion = api1.get('/v1/5')
      assert 5 == len(json.loads(peticion.data))
      
   def test_API1Arima_SiEsNumeroEntreCeroY72DevuelveCodigo200(self):
      peticion = api1.get('/v1/5')
      assert 200 == peticion.status_code
      
   def test_API1Arima_SiEsNumeroMayorQue72DevuelveCodigo400(self):
      peticion = api1.get('/v1/73')
      assert 400 == peticion.status_code

   def test_API1Arima_SiNoEsNumeroDevuelveCodigo404(self):
      peticion = api1.get('/v1/asd')
      assert 404 == peticion.status_code
      
   def test_API2OpenWeather_PidoCincoHoras_DevuelveListaDeCincoElementos(self):
      peticion = api2.get('/v2/5')
      assert 5 == len(json.loads(peticion.data))
      
   def test_API2OpenWeather_SiEsNumeroEntreCeroY72DevuelveCodigo200(self):
      peticion = api2.get('/v2/5')
      assert 200 == peticion.status_code
      
   def test_API2OpenWeather_SiEsNumeroMayorQue72DevuelveCodigo400(self):
      peticion = api2.get('/v2/73')
      assert 400 == peticion.status_code

   def test_API2OpenWeather_SiNoEsNumeroDevuelveCodigo404(self):
      peticion = api2.get('/v2/asd')
      assert 404 == peticion.status_code
      
   def test_API1ArimaDB_PidoCincoHoras_DevuelveListaDeCincoElementos(self):
      peticion = api1BD.get('/v3/5')
      assert 5 == len(json.loads(peticion.data))
      
   def test_API1ArimaDB_SiEsNumeroEntreCeroY72DevuelveCodigo200(self):
      peticion = api1BD.get('/v3/5')
      assert 200 == peticion.status_code
      
   def test_API1ArimaDB_SiEsNumeroMayorQue72DevuelveCodigo400(self):
      peticion = api1BD.get('/v3/73')
      assert 400 == peticion.status_code

   def test_API1ArimaDB_SiNoEsNumeroDevuelveCodigo404(self):      
      peticion = api1BD.get('/v3/asd')
      assert 404 == peticion.status_code

if __name__ == '__main__':
   unittest.main()
