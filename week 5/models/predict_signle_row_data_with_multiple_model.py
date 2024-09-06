import joblib
from sklearn.preprocessing import StandardScaler
import pandas as pd

__author__ = 'Darshan Dhanani'

# feature scalling
dataset = pd.read_csv("./dataset/wine_data.csv")
X = dataset.iloc[:, 1:-1].values

sc = StandardScaler()
sc.fit(X)

# load model
XGboost = joblib.load("models/pkl/XGBoost.pkl")
DecisionTree_Classifier = joblib.load('models/pkl/DecisionTree_Classifier.pkl')
KernalSVM = joblib.load('models/pkl/KernalSVM.pkl')
KNeighborsClassifier = joblib.load('models/pkl/KNeighborsClassifier.pkl')
LogisticRegression = joblib.load('models/pkl/LogisticRegression.pkl')
naiveBayes = joblib.load('models/pkl/naiveBayes.pkl')
RandomForestClassifier = joblib.load('models/pkl/RandomForestClassifier.pkl')
SVM = joblib.load('models/pkl/SVM.pkl')

def process_data(array_of_data):
    result_set = []
    
    data = sc.transform(array_of_data)
       
    result_set.append(["XGBoost", XGboost.predict(data)])
    result_set.append(["DecisionTree Classifier", DecisionTree_Classifier.predict(data)])
    result_set.append(["KernalSVM", KernalSVM.predict(data)])
    result_set.append(["K-Neighbors Classifier", KNeighborsClassifier.predict(data)])
    result_set.append(["LogisticRegression", LogisticRegression.predict(data)])
    result_set.append(["naiveBayes", naiveBayes.predict(data)])
    result_set.append(["RandomForestClassifier", RandomForestClassifier.predict(data)])
    result_set.append(["SVM", SVM.predict(data)])
    
    return result_set
    