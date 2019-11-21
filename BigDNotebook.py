# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import *
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.types import IntegerType


#import nltk
#from nltk.stem import PorterStemmer
#nltk.download('wordnet')
#from nltk.stem.snowball import SnowballStemmer
#from nltk.corpus import stopwords
#from nltk.stem import WordNetLemmatizer
import matplotlib.pyplot as plt
import numpy as np
import scipy as sp

#import com.johnsnowlabs.nlp.base._
#import com.johnsnowlabs.nlp.annotator._

import sparknlp
from sparknlp.pretrained import PretrainedPipeline

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import *
from pyspark.sql.functions import rand,when

# COMMAND ----------

spark.version

# COMMAND ----------

spark=SparkSession.builder.appName('nlp').getOrCreate()

# COMMAND ----------

df1=spark.read.table('table1')
df3=spark.read.table('table3')
#df1.count()
#df3.count()
dfinal= df1.union(df3)
#dfinal.count()
#dfinal.show(5)

# COMMAND ----------

dpandas = dfinal.select('*').toPandas()

# COMMAND ----------

dpandas['content'] = dpandas['content'].str.replace('\W', ' ')
dpandas['title'] = dpandas['title'].str.replace('\W', ' ')
dpandas['content'] = dpandas['content'].str.replace(' +', ' ')

# COMMAND ----------

dfSpark = spark.createDataFrame(dpandas)
#dfSpark.show(5)

# COMMAND ----------

tokenization=Tokenizer(inputCol='content',outputCol='tokens')
tokenized_df=tokenization.transform(dfSpark)
tokenized_df.show(10)

# COMMAND ----------

stopword_removal=StopWordsRemover(inputCol='tokens',outputCol='refined_tokens')

# COMMAND ----------

dfSpark=stopword_removal.transform(tokenized_df)

# COMMAND ----------

dfSpark.select(['id','tokens','refined_tokens']).show(10)

# COMMAND ----------

dfSpark.filter(((dfSpark.publication =='New York Times') | (dfSpark.publication =='Vox')))

# COMMAND ----------

display(dfSpark.groupBy('publication').count())
#display(dfSpark.groupBy('year').count())


# COMMAND ----------

sparknlp.start()
pipeline = PretrainedPipeline('analyze_sentiment', 'en')

# COMMAND ----------

dfSpark = dfSpark.withColumn('Sentimiento', when(rand() > 0.5, 1).otherwise(0))

# COMMAND ----------

dfSpark = dfSpark.withColumn("label", dfSpark.Sentimiento.cast('float')).drop('Sentimiento')

# COMMAND ----------

dfSpark.orderBy(rand()).show(10)

# COMMAND ----------

length = dfSpark.count()
lista_sentimientos = []
#Como parametro de range solo seria necesario cambiarlo por la variable LENGTH para que evalue todo el dataset
for i in range(3):
  columna_content = dfSpark.select('content').collect()[i].__getitem__("content")
  pred = pipeline.annotate(columna_content)
  if pred['sentiment'][0] == 'positive':
    lista_sentimientos.append('1')
  elif pred['sentiment'][0] == 'negative':
    lista_sentimientos.append('0')
  #print(pred['sentiment'])
#print(lista_sentimientos)
spark.createDataFrame(lista_sentimientos, StringType()).show()
#print(dfSpark.show())

# COMMAND ----------

len_udf = udf(lambda s: len(s), IntegerType())
dfSpark = dfSpark.withColumn("token_count", len_udf(col('refined_tokens')))
dfSpark.orderBy(rand()).show(10)

# COMMAND ----------

from pyspark.ml.feature import CountVectorizer

# COMMAND ----------

count_vec=CountVectorizer(inputCol='refined_tokens',outputCol='features')
dfSpark_V=count_vec.fit(dfSpark).transform(dfSpark)
dfSpark_V.select(['refined_tokens','token_count','features','Label']).show(10)

# COMMAND ----------

model_df=dfSpark_V.select(['features','token_count','Label'])

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

df_assembler = VectorAssembler(inputCols=['features','token_count'],outputCol='features_vec')
model_df = df_assembler.transform(model_df)

# COMMAND ----------


model_df.printSchema()

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

# COMMAND ----------

training_df,test_df=model_df.randomSplit([0.25,0.75])

# COMMAND ----------

training_df.groupBy('Label').count().show()

# COMMAND ----------

test_df.groupBy('Label').count().show()

# COMMAND ----------

log_reg=LogisticRegression(featuresCol='features_vec',labelCol='Label').fit(training_df)
results=log_reg.evaluate(test_df).predictions
results.show()

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator

# COMMAND ----------

true_postives = results[(results.Label == 1) & (results.prediction == 1)].count()
true_negatives = results[(results.Label == 0) & (results.prediction == 0)].count()
false_positives = results[(results.Label == 0) & (results.prediction == 1)].count()
false_negatives = results[(results.Label == 1) & (results.prediction == 0)].count()

# COMMAND ----------

recall = float(true_postives)/(true_postives + false_negatives)
print(recall)

# COMMAND ----------

precision = float(true_postives) / (true_postives + false_positives)
print(precision)

# COMMAND ----------

accuracy=float((true_postives+true_negatives) /(results.count()))
print(accuracy)
