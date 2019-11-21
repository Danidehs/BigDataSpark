# Proyecto 4 - Big Data con Spark

### Autores
El equipo está conformado por:

Daniel Enrique Hernández Stalhuth :shipit:
- dehernands@eafit.edu.co  
- hsdaniele95@gmail.com  

Ricardo Ocampo Castro :goat:
- rocampo3@eafit.edu.co 
- rocampocastro604@gmail.com  

Alejandro Gil Maya :trollface:
- ggilmay@eafit.edu.co 
- lordbluebanana@gmail.com
______

### Procesos del Proyecto

El desarrollo del proyecto se dividio en tres(3) grandes partes:
- Ingesta de datos
- Preparacion de datos
- Procesamiento de los datos

Como entorno se uso Databricks pues el equipo considero que al no tener recursos en AWS sería bueno aprovechar este proyecto para conocer este ambiente.

La primera parte consistia en obtener los datos de la base de datos all-news en formato csv (Esto es un formato en el que la estructura se encuentra separada por comas (,).
Ademas este dataset se encontraba separado en varios datasets, por lo que fue necesario recurrir a funciones de SQL para lograr unirlos en uno solo, que resulto en uno consolidado de alrededor de 100000 registros.

Luego se paso a la parte de preparacion y limpieza de datos donde se aplicaron tecnicas como eliminacion de signos de puntuacion y stopWords, para esto se convirtio el Spark Datafrrame en un Pandas Dataframe, pues para esta tarea no se encontraba tanta informacion y parecia mas sencillo hacerlo en Pandas.

Sucesivamente, para el procesamiento de los datos se volvio a convertir el Pandas Dataframe en un Spark Dataframe para asi aplicar un algoritmo de Analisis de sentimientos, pues lo que se busca es analizar si una noticia es buena o mala dependiendo de su descripcion (En este caso la descripcion estaba dada por la columna 'content' del Dataframe(dfSpark)), adicionalmente el equipo convirtio ese resultado que es tipo String en un Booleano para asi usar un modelo donde al aplicarle una Regresion logistica logre predecir si una noticia futura sera positiva o negativa.

## Grafico de articulos publicados por medio de comunicacion
![alt text](https://github.com/Danidehs/BigDataSpark/blob/master/Screenshots/big1.PNG)
## Tabla de columna content tokenisada
![alt text](https://github.com/Danidehs/BigDataSpark/blob/master/Screenshots/big2.PNG)
## Tabla de tokens con StopWords removidas
![alt text](https://github.com/Danidehs/BigDataSpark/blob/master/Screenshots/big3.PNG)
## Tabla con columna de analisis de sentimiento de los articulos
![alt text](https://github.com/Danidehs/BigDataSpark/blob/master/Screenshots/big4.PNG)
## Tabla de demostracion del algoritmo de analisis de sentimiento de cada uno de los articulos hasta el 3
cada uno tardaba 1 minuto o mas y para mas de 90000 articulos solo es posible con paralelismo MPI
![alt text](https://github.com/Danidehs/BigDataSpark/blob/master/Screenshots/big5.PNG)
