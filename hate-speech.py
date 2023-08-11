from datasets import load_dataset
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from transformers import BertTokenizer, BertModel
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix, recall_score
import torch
import numpy as np

# Configura una sesión de Spark con más memoria asignada
spark = SparkSession.builder \
    .appName("HateSpeechDetectionBERT") \
    .config("spark.driver.memory", "16g") \
    .config("spark.executor.memory", "16g") \
    .getOrCreate()

# Carga el conjunto de datos "hate_speech_offensive"
dataset = load_dataset("hate_speech_offensive")

# Accede al conjunto de entrenamiento
train_data = dataset["train"]

# Convierte el conjunto de datos a un DataFrame de Pandas
pandas_df = train_data.to_pandas()

# Limita el DataFrame a aproximadamente el 50% de los datos
pandas_df = pandas_df.sample(frac=0.5, random_state=42)

# Convierte el DataFrame de Pandas de nuevo a un objeto Dataset
train_data_with_tokens = spark.createDataFrame(pandas_df)

# Crea una etapa de tokenización utilizando BERT Tokenizer
tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")
tokenize_udf = udf(lambda text: tokenizer.encode(text, add_special_tokens=True), ArrayType(IntegerType()))
train_data_with_tokens = train_data_with_tokens.withColumn("token_ids", tokenize_udf("tweet"))

# Carga el modelo pre-entrenado de BERT
bert_model = BertModel.from_pretrained("bert-base-uncased")

# Extrae las características utilizando BERT
def extract_features(tokens):
    input_ids = torch.tensor(tokens).unsqueeze(0)
    with torch.no_grad():
        outputs = bert_model(input_ids)
    features = outputs.last_hidden_state.mean(1).squeeze().numpy()
    return features

extract_features_udf = udf(lambda tokens: extract_features(tokens), ArrayType(FloatType()))
train_data_with_features = train_data_with_tokens.withColumn("features", extract_features_udf("token_ids"))

# Define una función UDF para asignar los pesos según la clase y la técnica de mitigación de sesgo
class_weights = [1.0, 1.0, 1.5]  # Peso mayor para la clase "neither"
assign_weight_udf = udf(lambda c: class_weights[c], FloatType())

# Agrega la columna de pesos al DataFrame
train_data_with_weights = train_data_with_features.withColumn("weight", assign_weight_udf("class"))

# Crea un ensamblador de vectores para combinar las características en un solo vector
assembler = VectorAssembler(inputCols=["features"], outputCol="features_vector")
train_data_with_vector = assembler.transform(train_data_with_weights)

# Entrena un modelo de Regresión Logística utilizando las características de BERT
lr = LogisticRegression(featuresCol="features_vector", labelCol="class", predictionCol="prediction", weightCol="weight")
pipeline = Pipeline(stages=[lr])
model = pipeline.fit(train_data_with_vector)

# Realiza predicciones en el conjunto de entrenamiento
predictions = model.transform(train_data_with_vector)

# Calcula el F1-Score y el Recall
evaluator = MulticlassClassificationEvaluator(labelCol="class", predictionCol="prediction", metricName="f1")
f1_score = evaluator.evaluate(predictions)

y_true = predictions.select("class").rdd.flatMap(lambda x: x).collect()
y_pred = predictions.select("prediction").rdd.flatMap(lambda x: x).collect()
recall = recall_score(y_true, y_pred, average='weighted')

# Calcula la matriz de confusión
conf_matrix = confusion_matrix(y_true, y_pred)

# Muestra los resultados
print("F1-Score en el conjunto de entrenamiento: {:.4f}".format(f1_score))
print("Recall en el conjunto de entrenamiento:    {:.4f}".format(recall))
print("Matriz de Confusión:")
print(conf_matrix)

# Visualiza la matriz de confusión
plt.figure(figsize=(8, 6))
sns.heatmap(conf_matrix, annot=True, fmt="d", cmap="Blues", xticklabels=["0", "1", "2"], yticklabels=["0", "1", "2"])
plt.xlabel("Predicted")
plt.ylabel("True")
plt.title("Matriz de Confusión")
plt.show()

# Cierra la sesión de Spark
spark.stop()
