from flask import Flask, render_template, request, jsonify
import pandas as pd
import numpy as np
import models.predict_signle_row_data_with_multiple_model as pred

__author__ = 'Darshan Dhanani'

app = Flask(__name__, static_url_path="/static", static_folder='static')

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/dataset")
def dataset():
    datas = pd.read_csv('dataset/wine_data.csv').iloc[:, 1:]
    return render_template("dataset.html", datas=datas)
    
    
@app.route("/trained_model")
def trained_model():
    return render_template("trained_model.html")
    
    
@app.route("/predict_data")
def predict_data():
    return render_template("predict_data.html")
    
    
@app.route("/predict", methods=['POST'])
def predict():
    # convert form value into array
    features = [(x) for x in request.form.values()]
    f_features = [np.array(features)]

    print(f_features)
    # make predication with multiple model
    predicted_data = pred.process_data(f_features)
    
    return render_template("predict_data.html", datas=[predicted_data, features])
    

@app.route("/api/dataset", methods=['GET'])
def dataset_api():
    datas = pd.read_csv('dataset/wine_data.csv').iloc[:, 1:]
    data_dict = datas.to_dict(orient='records')
    return jsonify(data_dict)
    
    
@app.route("/api/predict-wine-class", methods=['POST'])
def predict_api():
    # convert form value into array
    features = [[(x) for x in request.form.values()]]

    # make predication with multiple model
    predicted_data = pred.process_data(features)
    
    response = [{'model': item[0], 'prediction class': float(item[1])} for item in predicted_data]
    return jsonify(response)

    
if __name__ == '__main__':
    app.run(debug=True)