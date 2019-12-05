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

# CAMBIAR LOS VALORES DE LAS COLUMNAS POR LABORABLE O FESTIVO
CSV = CSV.withColumn("DIA SEMANA", F.when(F.col("DIA SEMANA") == "LUNES", "LABORABLE").otherwise(F.col("DIA SEMANA")))
CSV = CSV.withColumn("DIA SEMANA", F.when(F.col("DIA SEMANA") == "MARTES", "LABORABLE").otherwise(F.col("DIA SEMANA")))
CSV = CSV.withColumn("DIA SEMANA", F.when(F.col("DIA SEMANA") == "MIERCOLES", "LABORABLE").otherwise(F.col("DIA SEMANA")))
CSV = CSV.withColumn("DIA SEMANA", F.when(F.col("DIA SEMANA") == "JUEVES", "LABORABLE").otherwise(F.col("DIA SEMANA")))
CSV = CSV.withColumn("DIA SEMANA", F.when(F.col("DIA SEMANA") == "VIERNES", "LABORABLE").otherwise(F.col("DIA SEMANA")))
CSV = CSV.withColumn("DIA SEMANA", F.when(F.col("DIA SEMANA") == "SABADO", "FESTIVO").otherwise(F.col("DIA SEMANA")))
CSV = CSV.withColumn("DIA SEMANA", F.when(F.col("DIA SEMANA") == "DOMINGO", "FESTIVO").otherwise(F.col("DIA SEMANA")))

# AGRUPAR POR DIA DE LA SEMANA, Y DESPUES SACAR LOS PORCENTAJES
CSV = CSV.groupby("DIA SEMANA").count().withColumn("PERCENTAJE", col("count") * 100.0 / (CSV.select("DIA SEMANA").count()))
CSV = CSV.withColumnRenamed("count", "NUMERO")

# SACAR LA TABLA POR CONSOLA
CSV.show()

# DIBUJAR LA GRAFICA
tipos = CSV.select('DIA SEMANA').collect()
valores = CSV.select('PERCENTAJE').collect()

labels = [tipos[0]['DIA SEMANA'], tipos[1]['DIA SEMANA']]
sizes = [valores[0]['PERCENTAJE'], valores[1]['PERCENTAJE']]

plt.figure(figsize = (800 / 96, 500 / 96), dpi = 96)

colors = ['yellowgreen', 'lightskyblue']
patches, texts = plt.pie(sizes, colors = colors, shadow = True, startangle = 90)

# Equal aspect ratio ensures that pie is drawn as a circle.
plt.axis('equal')
plt.legend(patches, labels, loc = "best")
plt.tight_layout()	
plt.savefig('diaLaborable.png', dpi = 96)