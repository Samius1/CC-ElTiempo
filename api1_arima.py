import flask
import GestorDatos
import json

app = flask.Flask(__name__)
app.config["DEBUG"] = True
gestorDatos = GestorDatos.GestorDatos()

@app.route('/', methods=['GET'])
def home():
   return "<h1>API utilizando ARIMA</h1><p>API de ejemplo para obtener la predicción del tiempo utilizando ARIMA.</p>"

@app.route('/v1/<int:horas>', methods=['GET'])
def arima(horas):
   if (horas <= 0 or horas > 72):
      return flask.Response('Número incorrecto. Introduzca un número mayor que 0 y menor que 73.', status=400)
   
   gestorDatos.CreacionDeModelos('')
   modelos = gestorDatos.ObtenerModelos('')
         
   modeloHumedadFinal = modelos[0]
   modeloTemperaturaFinal = modelos[1]   
         
   prediccionHumedad, conf = modeloHumedadFinal.predict(horas, return_conf_int=True)
   prediccionTemperatura, confint = modeloTemperaturaFinal.predict(horas, return_conf_int=True)   
   rango_horas = gestorDatos.CrearRangoHoras(horas + 1)
   
   resultadoArima = []
   
   for hora, temperatura, humedad in zip(rango_horas, prediccionTemperatura, prediccionHumedad):
      resultadoArima.append( { "hour": hora.strftime('%H:00'), "temp": round(temperatura,2), "hum": round(humedad,2) })
   
   if (len(resultadoArima) == 0):
      return flask.Response("No se ha encontrado información.", status=400)
   else:
      return flask.Response(json.dumps(resultadoArima), mimetype='application/json', status=200)
