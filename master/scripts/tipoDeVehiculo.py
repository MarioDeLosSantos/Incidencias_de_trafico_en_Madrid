# SPARK
from pyspark import SparkConf, SparkContext, SQLContext
import pyspark
# MORE SPARK
from pyspark.sql.functions import rank, sum, col
from pyspark.sql import Window
# PARA LAS GRAFICAS
import matplotlib.pyplot as plt

# CONFIGURACION
conf = SparkConf().setMaster('local[*]').setAppName('tipoDeVehiculo')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# COGE EL ARCHIVO
CSV = sqlContext.read.format("csv").option("header", "true").option("Inferschema", "true").load("MadridAccidents.csv")

# AGRUPAR POR DIA DE LA SEMANA, Y DESPUES SACAR LOS PORCENTAJES
CSV = CSV.groupby("Tipo Vehiculo").count().withColumn("PERCENTAJE", col("count") * 100.0 / (CSV.select("Tipo Vehiculo").count()))
CSV = CSV.withColumnRenamed("count", "NUMERO")

# SACAR LA TABLA POR CONSOLA
CSV.show()

# DIBUJAR LA GRAFICA
tipos = CSV.select('Tipo Vehiculo').collect()
valores = CSV.select('PERCENTAJE').collect()

labels = [tipos[0]['Tipo Vehiculo'],
tipos[1]['Tipo Vehiculo'],
tipos[2]['Tipo Vehiculo'],
tipos[3]['Tipo Vehiculo'],
tipos[4]['Tipo Vehiculo'],
tipos[5]['Tipo Vehiculo'],
tipos[6]['Tipo Vehiculo'],
tipos[7]['Tipo Vehiculo'],
tipos[8]['Tipo Vehiculo'],
tipos[9]['Tipo Vehiculo'],
tipos[10]['Tipo Vehiculo']]
sizes = [valores[0]['PERCENTAJE'],
valores[1]['PERCENTAJE'],
valores[2]['PERCENTAJE'],
valores[3]['PERCENTAJE'],
valores[4]['PERCENTAJE'],
valores[5]['PERCENTAJE'],
valores[6]['PERCENTAJE'],
valores[7]['PERCENTAJE'],
valores[8]['PERCENTAJE'],
valores[9]['PERCENTAJE'],
valores[10]['PERCENTAJE']]

plt.figure(figsize = (800 / 96, 500 / 96), dpi = 96)

colors = ['darkorange', 'lightskyblue', 'gold', 'lightcoral', 'blueviolet', 'grey', 'lightgreen', 'midnightblue', 'lightyellow', 'lime', 'green']
patches, texts = plt.pie(sizes, colors = colors, shadow = True, startangle = 90)

# Equal aspect ratio ensures that pie is drawn as a circle.
plt.axis('equal')
plt.legend(patches, labels, loc = "best")
plt.tight_layout()	
plt.savefig('tipoDeVehiculo.png', dpi = 96)