import numpy as np
from tensorflow.keras.models import load_model

MODEL_PATH = "../ml_models/baseline_model.joblib"

model = load_model(MODEL_PATH)


def predict_next(sequence):
    """
    sequence: numpy array (timesteps, features)
    """
    sequence = np.expand_dims(sequence, axis=0)
    return model.predict(sequence)[0][0]
