import numpy as np
import pandas as pd
import tensorflow as tf
import os
import joblib

from google.cloud import storage

from tensorflow.keras.layers import Dense
from tensorflow.keras.models import Sequential

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler, OneHotEncoder

BUCKET_NAME = "[project]"
GCS_FOLDER = "adult-income-cc-training-model"
DATA_FILE = "adult-income.csv"

# Load the data
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)
blob = bucket.blob(DATA_FILE)
blob.download_to_filename(DATA_FILE)
data = pd.read_csv(DATA_FILE)

# Exclude 'functional_weight' and 'income_bracket' from features
features = ['age', 'workclass', 'education', 'education_num', 'marital_status', 'occupation', 
            'relationship', 'race', 'sex', 'capital_gain', 'capital_loss', 'hours_per_week', 'native_country']
X = data[features].values
y = data['income_bracket'].values

# Encode the string labels to integers
label_encoder = LabelEncoder()
y_encoded = label_encoder.fit_transform(y)

# Identify categorical features
categorical_features = ['workclass', 'education', 'marital_status', 'occupation', 
                        'relationship', 'race', 'sex', 'native_country']

# OneHotEncode the categorical features
categorical_encoder = OneHotEncoder(sparse_output=False)
categorical_encoded = categorical_encoder.fit_transform(data[categorical_features])

# Combine the numerical features with the encoded categorical features
numerical_features = ['age', 'education_num', 'capital_gain', 'capital_loss', 'hours_per_week']
X_combined = np.hstack((data[numerical_features].values, categorical_encoded))

# Split the data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(
    X_combined, y_encoded, test_size=0.2, random_state=42)

# Scale the numerical features in the training data
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train[:, :len(numerical_features)])
X_test_scaled = scaler.transform(X_test[:, :len(numerical_features)])

# Combine the scaled numerical features with the encoded categorical features
X_train_final = np.hstack((X_train_scaled, X_train[:, len(numerical_features):]))
X_test_final = np.hstack((X_test_scaled, X_test[:, len(numerical_features):]))

# Save the scaler and encoders for later use during prediction
joblib.dump(scaler, 'scaler.pkl')
joblib.dump(label_encoder, 'label_encoder.pkl')
joblib.dump(categorical_encoder,'categorical_encoder.pkl')

blob = bucket.blob(f"{GCS_FOLDER}/scaler.pkl")
blob.upload_from_filename("./scaler.pkl")

blob = bucket.blob(f"{GCS_FOLDER}/label_encoder.pkl")
blob.upload_from_filename("./label_encoder.pkl")

blob = bucket.blob(f"{GCS_FOLDER}/categorical_encoder.pkl")
blob.upload_from_filename("./categorical_encoder.pkl")

# Define the model
model = Sequential([
    Dense(64, activation='relu', input_shape=(X_train_final.shape[1],)),
    Dense(64, activation='relu'),
    Dense(len(label_encoder.classes_), activation='softmax')
])

# Compile the model
model.compile(optimizer='adam',
              loss='sparse_categorical_crossentropy', metrics=['accuracy'])

# Train the model
model.fit(X_train_final, y_train, epochs=1, validation_split=0.2)
MODEL_DIR = os.getenv("AIP_MODEL_DIR")
model.save(MODEL_DIR)