from pyspark import SparkConf, SparkContext,SQLContext
import string
import pyspark
import matplotlib.pyplot as plt

from pyspark.sql.functions import rank,sum,col
from pyspark.sql import Window

#Configuracion de Spark, usando todos los cores disponibles
conf=SparkConf().setMaster('local[*]').setAppName('tipoDeColision')
sc=SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#Cargamos y guardamos el CSV
table = sqlContext.read.format("csv").option("header","true").option("Inferschema","true").load("MadridAccidents.csv")

#Eliminamos los accidentes duplicados quitando aquellos que tengan la misma fecha, mismo rango horario y mismo lugar
table=table.dropDuplicates(["FECHA","RANGO HORARIO"])
table=table.dropDuplicates(["LUGAR ACCIDENTE"])
	
#Ordenamos y sacamos el porcentaje de los accidentes totales
table=table.groupby("TIPO ACCIDENTE").count().withColumn("%",col("count")*100/(table.select("TIPO ACCIDENTE").count()))

#DIBUJAR LA GRAFICA

tipos=table.select('TIPO ACCIDENTE').collect()
valores=table.select('%').collect()

labels = [tipos[0]['TIPO ACCIDENTE'], tipos[1]['TIPO ACCIDENTE'], tipos[2]['TIPO ACCIDENTE'], tipos[3]['TIPO ACCIDENTE'],tipos[4]['TIPO ACCIDENTE'],tipos[5]['TIPO ACCIDENTE'],
tipos[6]['TIPO ACCIDENTE'],tipos[7]['TIPO ACCIDENTE'],tipos[8]['TIPO ACCIDENTE'],tipos[9]['TIPO ACCIDENTE']]
sizes = [valores[0]['%'], valores[1]['%'], valores[2]['%'], valores[3]['%'],valores[4]['%'],valores[5]['%'],valores[6]['%'],valores[7]['%'],valores[8]['%'],
valores[9]['%']]

plt.figure(figsize=(800/96, 500/96), dpi=96)

colors = ['yellowgreen', 'gold', 'lightskyblue', 'lightcoral','blueviolet','red','blue','green','yellow']
patches, texts = plt.pie(sizes,colors=colors, shadow=True, startangle=90)


plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.legend(patches, labels, loc="best")
plt.tight_layout()	
plt.savefig('tipoDeColision.png',dpi=96)
