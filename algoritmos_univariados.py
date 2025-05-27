import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
from sklearn.decomposition import PCA

# 1. Hotelling T² univariado
def hotelling_T2_univariado(df, alpha=0.01):
    x = df['valor']
    mean = x.mean()
    std = x.std()
    T2 = ((x - mean) / std) ** 2
    umbral = np.percentile(T2, 100 * (1 - alpha))
    df["T2"] = T2
    df["anomalía"] = df["T2"] > umbral
    return df

# 2. Isolation Forest
def isolation_forest(df, contamination=0.01, random_state=42):
    modelo = IsolationForest(contamination=contamination, random_state=random_state)
    df = df.copy()
    df['valor'] = df['valor'].astype(float)
    df["anomalía"] = modelo.fit_predict(df[['valor']])
    df["anomalía"] = df["anomalía"] == -1
    return df

# 3. Z-score con ventana móvil
def rolling_zscore(df, window=48, threshold=4.0, std_threshold=0.05, range_threshold=0.1):
    df = df.copy()
    valores = df['valor']

    # Verificar si la serie es prácticamente constante
    desviacion_global = valores.std()
    rango_global = valores.max() - valores.min()

    if desviacion_global < std_threshold or rango_global < range_threshold:
        print(f"⚠️ Señal constante detectada: std={desviacion_global:.5f}, rango={rango_global:.5f}. Z-score no aplicado.")
        df['anomalía'] = False
        return df

    # Aplicar z-score si la serie tiene suficiente variabilidad
    df['media_movil'] = valores.rolling(window=window).mean()
    df['std_movil'] = valores.rolling(window=window).std()
    df['zscore'] = (valores - df['media_movil']) / df['std_movil'].replace(0, np.nan)
    df['anomalía'] = df['zscore'].abs() > threshold

    return df.drop(columns=['media_movil', 'std_movil', 'zscore'])

# 4. Mediana móvil + MAD
def rolling_mad(df, window=48, threshold=5.0, min_mad=0.05):
    df = df.copy()
    df['mediana'] = df['valor'].rolling(window=window).median()
    df['mad'] = df['valor'].rolling(window=window).apply(
        lambda x: np.median(np.abs(x - np.median(x))), raw=True
    )
    df['mad'] = df['mad'].clip(lower=min_mad)  # evita MAD demasiado pequeñas
    df['score'] = np.abs(df['valor'] - df['mediana']) / df['mad']
    df['anomalía'] = df['score'] > threshold
    return df
# 5. One-Class SVM
def one_class_svm(df, nu=0.01, gamma='scale'):
    df = df.copy()
    df['valor'] = df['valor'].astype(float)
    modelo = OneClassSVM(nu=nu, gamma=gamma)
    modelo.fit(df[['valor']])
    df["anomalía"] = modelo.predict(df[['valor']]) == -1
    return df

# 6. PCA + Hotelling T² (en ventanas)
def pca_hotelling(df, window_size=24, alpha=0.01):
    X = []
    fechas = []
    for i in range(len(df) - window_size + 1):
        ventana = df['valor'].iloc[i:i+window_size].values
        X.append(ventana)
        fechas.append(df['fecha_hora'].iloc[i + window_size - 1])

    X = np.array(X)
    pca = PCA(n_components=1)
    X_pca = pca.fit_transform(X)
    T2 = (X_pca - X_pca.mean())**2 / X_pca.std()**2
    umbral = np.percentile(T2, 100 * (1 - alpha))

    resultados = pd.DataFrame({
        "fecha_hora": fechas,
        "T2": T2.flatten(),
        "anomalía": T2.flatten() > umbral
    })
    return resultados
