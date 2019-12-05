from pyspark import SparkConf, SparkContext,SQLContext
import string
import pyspark
import matplotlib.pyplot as plt

from pyspark.sql.functions import rank,sum,col
from pyspark.sql import Window

#Configuracion de Spark, usando todos los cores disponibles
conf=SparkConf().setMaster('local[*]').setAppName('rangoHorario')
sc=SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#Cargamos y guardamos el CSV
table = sqlContext.read.format("csv").option("header","true").option("Inferschema","true").load("MadridAccidents.csv")
	
#Ordenamos y sacamos el numero de accidentes por cada tipo de clima
granizo=table.groupby("CPFA Granizo").count().select('count').collect()[1]['count']
niebla=table.groupby("CPFA Niebla").count().select('count').collect()[1]['count']
lluvia=table.groupby("CPFA Lluvia").count().select('count').collect()[1]['count']
hielo=table.groupby("CPFA Hielo").count().select('count').collect()[1]['count']

#Dibujamos y guardamos la grafica

labels ='Granizo','Niebla','Lluvia','Hielo'
sizes = [granizo,niebla,lluvia,hielo]

colors = ['yellowgreen', 'gold', 'lightskyblue', 'lightcoral']
patches, texts = plt.pie(sizes,colors=colors, shadow=True, startangle=90)


plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.legend(patches, labels, loc="best")
plt.tight_layout()
plt.savefig('TipoDeClimaNoFavorable.png')
