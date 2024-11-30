# Truck Delay Classification: End-to-End Machine Learning Application
Modern machine learning project development - This project provides the real-time delay updates to logistic companies. The application utilizes MLflow for model tracking and management, Hopsworks Feature Store for storing and managing the dataset, and Streamlit for building an interactive web application to predict truck delays.

## Overview
The Truck Delay Classification application aims to predict whether a truck will experience a delay during its journey based on several influencing factors, such as the truck’s ID, route ID, and the departure date.

## Key components:

- **Data Pipeline**: Data is ingested from Hopsworks Feature Store, cleaned, and transformed before feeding it into the ML model.
- **Model Training & Management**: The machine learning model is registered and tracked using MLflow, ensuring version control and easy access to the best-performing model.
- **Streamlit Application**: A user-friendly web interface built using Streamlit allows users to interact with the model by filtering data based on truck ID, route ID, or date range to predict potential delays.

## Features
- **Data Ingestion**: Pull data from the Hopsworks Feature Store to train the machine learning model.
- **Model Training and Tracking**: Use MLflow to manage the model training process and store model artifacts (e.g., encoders, scalers, and the model itself).
- **Interactive Web Application**: Built with Streamlit, the application allows users to:
    - Filter data by Date Range, Truck ID, or Route ID.
    - Predict truck delays for the selected filters using a pre-trained model.
- **Model Inference**: The application uses a pre-trained machine learning model to predict delays in real-time.

## **Architecture**
- Data: The final dataset is stored in the Hopsworks Feature Store.
- Model: The model is registered in MLflow Model Registry, enabling easy management and versioning.
- Inference: The Streamlit application fetches the data, processes it, and runs predictions using the MLflow model.
- UI: Streamlit provides an intuitive interface for users to interact with the application.

## **Prerequisites**

- python version : 3.10.2 or Later
- Library Requirements
- pymysql==1.1.0
- psycopg2==2.9.7
- pandas==1.5.3
- numpy==1.23.5
- matplotlib==3.7.1
- seaborn==0.12.2
- hopsworks==3.2.0
- scikit-learn
- xgboost
- MLflow: For model training, versioning, and tracking.
- Hopsworks: For storing features and retrieving the dataset.
- Streamlit: For building the web interface.
- Install the required libraries using pip: `pip install -r requirements.txt`

## **Setup and Configuration**
### **1. Clone the Repository**
Clone this repository to your local machine or server: `git clone https://github.com/yourusername/truck-delay-classification.git`
- cd truck-delay-classification
### **2. Hopsworks Setup**
- Login to Hopsworks:
  - Create an account on Hopsworks.
  - Set up a new project in Hopsworks.
- Upload the final merged truck delay dataset to the Feature Store in your Hopsworks project.
- Update the Feature Store Code: In app.py, modify the following code snippet to connect to your Hopsworks project: python


# Connect to Hopsworks
--bash

# Connect to Hopsworks

To connect to the **Hopsworks Feature Store** and retrieve the final merged dataset for predictions, use the following Python code:

```python
import hopsworks

# Login to Hopsworks
project = hopsworks.login()

# Access the feature store
feature_store = project.get_feature_store()

# Retrieve the dataset
final_merge = feature_store.get_dataframe("truck_delay_features")


`import hopsworks
project = hopsworks.login()
feature_store = project.get_feature_store()
final_merge = feature_store.get_dataframe("truck_delay_features") `

### **3. MLflow Setup**
- Install and configure MLflow.
- Train the model using XGBoost, Random Forest algorithms
- Save the model and register it to MLflow’s Model Registry.
- Save the model and preprocessing artifacts (encoder and scaler) in the MLflow registry. 

`import mlflow
import mlflow.sklearn
model = train_model()
# Log the model in MLflow
mlflow.sklearn.log_model(model, "truck-delay-classification-model")`
- Update the app.py to point to the correct model in the MLflow Model Registry:

`model_uri = "models:/truck-delay-classification-model/1"
model = mlflow.sklearn.load_model(model_uri)`
### **4. Streamlit Application**
- launch the Streamlit application to predict truck delays.

### **5. Running the Application**
- To start the Streamlit app, run the following command in your terminal: `streamlit run app.py`
- The application will launch on your default web browser `(usually at http://localhost:8501)`.

Filtering Options
The user can filter the data based on:

Date Range: Filter predictions by a specific date range.
Truck ID: Choose a specific truck ID to predict its delay.
Route ID: Choose a specific route to predict delays for that route.
After selecting the desired filter, users can click on the "Predict" button to get the truck delay predictions.

Application Flow
Data Filtering: Based on the user input, the data is filtered by date, truck ID, or route ID.
Model Inference: The filtered data is passed to the pre-trained model to predict the likelihood of delay.
Result Display: The results are shown on the Streamlit UI, displaying the predicted truck delays.
Example Code (app.py)
Here’s the core Streamlit application code (app.py):

python
Copy code
import streamlit as st
import mlflow
import hopsworks
import pickle
import pandas as pd
from sklearn.preprocessing import StandardScaler

# Connect to Hopsworks
project = hopsworks.login()
feature_store = project.get_feature_store()
final_merge = feature_store.get_dataframe("truck_delay_features")

# Load the model and preprocessing artifacts
model_uri = "models:/truck-delay-classification-model/1"
model = mlflow.sklearn.load_model(model_uri)

# Load encoder and scaler
with open("encoder.pkl", "rb") as f:
    encoder = pickle.load(f)
with open("scaler.pkl", "rb") as f:
    scaler = pickle.load(f)

# Streamlit interface
st.title('Truck Delay Classification')

# Filtering options
options = ['date_filter', 'truck_id_filter', 'route_id_filter']
selected_option = st.radio("Choose an option:", options)

if selected_option == 'date_filter':
    st.write("### Date Ranges")
    from_date = st.date_input("Enter start date", min_value=min(final_merge['departure_date']))
    to_date = st.date_input("Enter end date", max_value=max(final_merge['departure_date']))

elif selected_option == 'truck_id_filter':
    st.write("### Truck ID")
    truck_id = st.selectbox('Select truck ID', final_merge['truck_id'].unique())

elif selected_option == 'route_id_filter':
    st.write("### Route ID")
    route_id = st.selectbox('Select route ID', final_merge['route_id'].unique())

if st.button('Predict'):
    try:
        if selected_option == 'date_filter':
            filter_query = (final_merge['departure_date'] >= str(from_date)) & (final_merge['departure_date'] <= str(to_date))
        elif selected_option == 'truck_id_filter':
            filter_query = (final_merge['truck_id'] == truck_id)
        elif selected_option == 'route_id_filter':
            filter_query = (final_merge['route_id'] == route_id)

        # Filter data
        filtered_data = final_merge[filter_query]
        X = filtered_data.drop('delay_class', axis=1)  # Assuming 'delay_class' is the target column
        X_scaled = scaler.transform(X)
        X_encoded = encoder.transform(X_scaled)

        # Predict delay
        predictions = model.predict(X_encoded)

        # Display results
        st.write("### Predictions:")
        st.write(predictions)

    except Exception as e:
        st.write(f"Error: {e}")



## **System Requirements**


## **PROJECT STRUCTURE**

This Project develement has been devided into 3 different phases

1. Data Ingestion and Preparation
2. Machine Learning Model building & hyper perameter tunning 
3. Model Deployment and Inference





