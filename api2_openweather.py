from datetime import datetime
import flask
import json
import requests

app = flask.Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
   return "<h1>API utilizando una API</h1><p>API de ejemplo para obtener la predicción del tiempo utilizando la API de Open Weather Map.</p>"

@app.route('/v2/<int:horas>', methods=['GET'])
def OpenWeather(horas):
   if (horas <= 0 or horas > 72):
      return flask.Response("Número incorrecto. Introduzca un número mayor que 0 y menor que 73.", status=400)
      
   apiURL = 'https://api.openweathermap.org/data/2.5/onecall?lat=37.733795&lon=-122.446747&exclude=current,minutely,daily,alerts&appid=068239bf400481fd239c99c359058e0e'
   
   peticion = requests.get(apiURL)
   
   datos = peticion.json()
   
   informacionHoras = datos['hourly']
   
   if (len(informacionHoras) == 0):
      return flask.Response("No se ha obtenido información de la API.", status=400)
   
   else:      
      horas = 48 if horas > 48 else horas
   
      resultadoOpenWeather = []
      
      for x in range(horas):
         resultadoOpenWeather.append( { "hour": datetime.utcfromtimestamp(informacionHoras[x]['dt']).strftime('%H:00'), "temp": round(informacionHoras[x]['temp'],2), "hum": round(informacionHoras[x]['humidity'],2) })
         
      return flask.Response(json.dumps(resultadoOpenWeather), mimetype='application/json', status=200)
