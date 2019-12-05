# SPARK
from pyspark import SparkConf, SparkContext, SQLContext
import pyspark
# MORE SPARK
from pyspark.sql import functions as F
from pyspark.sql.functions import rank, sum, col
from pyspark.sql import Window
# PARA LAS GRAFICAS
import matplotlib.pyplot as plt

# CONFIGURACION
conf = SparkConf().setMaster('local[*]').setAppName('diaLaborable')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# COGE EL ARCHIVO
CSV = sqlContext.read.format("csv").option("header", "true").option("Inferschema", "true").load("MadridAccidents.csv")

# AGRUPAR Y CONTAR POR NUMERO DE ACCIDENTES
MOJ = CSV.groupby("CPSV Mojada").count().select('count').collect()[1]['count']
ACE = CSV.groupby("CPSV Aceite").count().select('count').collect()[1]['count']
BAR = CSV.groupby("CPSV Barro").count().select('count').collect()[1]['count']
GRA = CSV.groupby("CPSV Grava Suelta").count().select('count').collect()[1]['count']
HIE = CSV.groupby("CPSV Hielo").count().select('count').collect()[1]['count']
SEC = CSV.groupby("CPSV Seca Y Limpia").count().select('count').collect()[1]['count']


# DIBUJAR LA GRAFICA
labels ='Mojada','Aceite','Barro','Granizo', 'Grava', 'Hielo', 'Seca'
sizes = [MOJ, ACE, BAR, GRA, HIE, SEC]

colors = ['yellowgreen', 'gold', 'lightskyblue', 'lightcoral', 'blueviolet', 'lightyellow']
patches, texts = plt.pie(sizes, colors = colors, shadow = True, startangle = 90)

# Equal aspect ratio ensures that pie is drawn as a circle.
plt.axis('equal')
plt.legend(patches, labels, loc = "best")
plt.tight_layout()
plt.savefig('estadoDeLaCalzada.png')