from statsmodels.tsa.arima_model import ARIMA
import datetime
import flask
import json
import pandas as p
import pmdarima as ari

app = flask.Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
   return "<h1>API utilizando ARIMA</h1><p>API de ejemplo para obtener la predicción del tiempo utilizando ARIMA.</p>"

@app.route('/v1/<int:horas>', methods=['GET'])
def arima(horas):
   if (horas <= 0):
      return flask.Response("Número incorrecto. Introduzca un número mayor que 0.", status=400)
      
   datos = p.read_csv('~/Desktop/SPA_Airflow/mezcla.csv', header=0)
   
   modeloTemperatura = ari.auto_arima(datos['TEMP'].dropna(), start_p=1, start_q=1, test='adf', max_p=3, max_q=3, m=1, d=None, seasonal=False, start_P=0, D=0, trace=True, error_action='ignore', suppress_warnings=True, stepwise=True)      
      
   modeloHumedad = ari.auto_arima(datos['HUM'].dropna(), start_p=1, start_q=1, test='adf', max_p=3, max_q=3, m=1, d=None, seasonal=False, start_P=0, D=0, trace=True, error_action='ignore', suppress_warnings=True, stepwise=True)      
      
   prediccionTemperatura, conf = modeloTemperatura.predict(horas, return_conf_int=True)
   prediccionHumedad, confint = modeloHumedad.predict(horas, return_conf_int=True)   
   rango_horas = p.date_range(start=datetime.datetime.now(), periods=horas + 1, freq='H')
   
   resultadoArima = []
   
   for hora, temperatura, humedad in zip(rango_horas, prediccionTemperatura, prediccionHumedad):
      resultadoArima.append( { "hour": hora.strftime('%H:00'), "temp": round(temperatura,2), "hum": round(humedad,2) })
   
   if (len(resultadoArima) == 0):
      return flask.Response("No se ha encontrado información.", status=400)
   else:
      return flask.Response(json.dumps(resultadoArima), mimetype='application/json', status=200)
    
app.run()
