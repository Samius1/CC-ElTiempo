from keras.models import Sequential
from keras.layers import LSTM, Dense, Dropout, Bidirectional
import flask
import json
import numpy as np
import pandas as p



app = flask.Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
   return "<h1>API utilizando NN</h1><p>ApI de ejemplo para obtener la predicción del tiempo utilizando NN, redes neuronales.</p>"

@app.route('/v2/<int:horas>', methods=['GET'])
def LSTM(horas):
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
      
   x_train, y_train = np.array(x_train), np.array(y_train)
   x_train = np.reshape(x_train, (x_train.shape[0], x_train.shape[1], 1))
   
   epochs = 10
   batch_size = 64
   
   modeloTemperatura = Sequential()
   modeloTemperatura.add(Bidirectional(LSTM(30)(return_sequences=True,input_shape=(x_train.shape[1],1))))
   modeloTemperatura.add(Dropout(0.2))
   modeloTemperatura.add(LSTM(30)(return_sequences=True))
   modeloTemperatura.add(Dropout(0.2))
   modeloTemperatura.add(LSTM(30)(return_sequences=True))
   modeloTemperatura.add(Dropout(0.2))
   modeloTemperatura.add(LSTM(30)(return_sequences=True))
   modeloTemperatura.add(Dropout(0.2))
   modeloTemperatura.add(Dense(units=horas, activation='relu'))
   
   modeloTemperatura.compile(optimizer='adam', loss='mean_square_error', metrics=['acc'])
   modeloTemperatura.fit(x_train, y_train, epochs=epochs, batch_size=batch_size)
   
   x_test = test_set[: horas, 0]
   y_test = test_set[0 : horas, 0]
   
   prediccionTemperatura = modeloTemperatura.predict(x_test)
   
   rango_horas = p.date_range(start=datetime.datetime.now(), periods=horas + 1, freq='H')
   
   resultadoLSTM = []
   
   for hora, temperatura in zip(rango_horas, prediccionTemperatura):
      resultadoLSTM.append( { "hour": hora.strftime('%H:00'), "temp": round(temperatura,2) })
   
   if (len(resultadoLSTM) == 0):
      return flask.Response("No se ha encontrado información.", status=400)
   else:
      return flask.Response(json.dumps(resultadoLSTM), mimetype='application/json', status=200)
    
app.run()
