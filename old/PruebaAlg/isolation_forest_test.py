import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest

def isolation_forest(df, contamination=0.01, random_state=42):
    """
    Aplica Isolation Forest a una serie temporal univariada.
    - df: DataFrame con columnas 'fecha_hora' y 'valor'
    - contamination: proporción esperada de outliers
    Retorna el DataFrame original con una columna 'anomalía' (True/False).
    """
    modelo = IsolationForest(contamination=contamination, random_state=random_state)
    df = df.copy()
    df['valor'] = df['valor'].astype(float)

    df["anomalía"] = modelo.fit_predict(df[['valor']])
    df["anomalía"] = df["anomalía"] == -1  # -1 = outlier, 1 = normal
    return df
