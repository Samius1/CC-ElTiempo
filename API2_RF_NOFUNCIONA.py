from sklearn.ensemble import RandomForestRegressor
import flask
import json
import numpy as np
import pandas as p


app = flask.Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
   return "<h1>API utilizando RF</h1><p>ApI de ejemplo para obtener la predicción del tiempo utilizando RF, random forest.</p>"

@app.route('/v2/<int:horas>', methods=['GET'])
def RF(horas):
   datos = p.read_csv('~/Desktop/SPA_Airflow/mezcla.csv', header=0)
   
   temperatura = datos[['TEMP']].dropna()
   
   humedad = datos['HUM'].dropna()
   
   # Esto puede ser que se elimine
   train_separator = 0.85
   split_idx = int(len(temperatura) * train_separator)
   
   training_set = temperatura[:split_idx].values
   test_set = temperatura[split_idx:].values
   	
   x_train = []
   y_train = []
   
   for i in range(0, len(training_set) - horas + 1):
      x_train.append(training_set[i : i, 0])
      y_train.append(training_set[i : i + horas , 0])
   
   epochs = 10
   batch_size = 64
   
   
   x_train = x_train.reshape(len(x_train), 1)
      
   modeloTemperatura = RandomForestRegressor(n_estimators=1000, random_state=42)   
   modeloTemperatura.fit(x_train, y_train)
   
   x_test = test_set[: horas, 0]
   y_test = test_set[0 : horas, 0]
   
   prediccionTemperatura = modeloTemperatura.predict(x_test)
   
   rango_horas = p.date_range(start=datetime.datetime.now(), periods=horas + 1, freq='H')
   
   resultadoRF = []
   
   for hora, temperatura in zip(rango_horas, prediccionTemperatura):
      resultadoRF.append( { "hour": hora.strftime('%H:00'), "temp": round(temperatura,2) })
   
   if (len(resultadoRF) == 0):
      return flask.Response("No se ha encontrado información.", status=400)
   else:
      return flask.Response(json.dumps(resultadoRF), mimetype='application/json', status=200)
    
app.run()
