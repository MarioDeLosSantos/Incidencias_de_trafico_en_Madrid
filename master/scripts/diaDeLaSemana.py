# Copyright 2019
# Authors: Mario de los Santos, Jesús Ramos, Marcos Docampo Prieto-Puga
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.


# SPARK
from pyspark import SparkConf, SparkContext, SQLContext
import pyspark
# MORE SPARK
from pyspark.sql.functions import rank, sum, col
from pyspark.sql import Window
# PARA LAS GRAFICAS
import matplotlib.pyplot as plt

# CONFIGURACION
conf = SparkConf().setMaster('local[*]').setAppName('diaDeLaSemana')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# COGE EL ARCHIVO
CSV = sqlContext.read.format("csv").option("header", "true").option("Inferschema", "true").load("MadridAccidents.csv")

# AGRUPAR POR DIA DE LA SEMANA, Y DESPUES SACAR LOS PORCENTAJES
CSV = CSV.groupby("DIA SEMANA").count().withColumn("PERCENTAJE", col("count") * 100.0 / (CSV.select("DIA SEMANA").count()))
CSV = CSV.withColumnRenamed("count", "NUMERO")

# SACAR LA TABLA POR CONSOLA
CSV.show()

# DIBUJAR LA GRAFICA
tipos = CSV.select('DIA SEMANA').collect()
valores = CSV.select('PERCENTAJE').collect()

labels = [tipos[0]['DIA SEMANA'],
tipos[1]['DIA SEMANA'],
tipos[2]['DIA SEMANA'],
tipos[3]['DIA SEMANA'],
tipos[4]['DIA SEMANA'],
tipos[5]['DIA SEMANA'],
tipos[6]['DIA SEMANA']]
sizes = [valores[0]['PERCENTAJE'],
valores[1]['PERCENTAJE'],
valores[2]['PERCENTAJE'],
valores[3]['PERCENTAJE'],
valores[4]['PERCENTAJE'],
valores[5]['PERCENTAJE'],
valores[6]['PERCENTAJE']]

plt.figure(figsize = (800 / 96, 500 / 96), dpi = 96)

colors = ['yellowgreen', 'lightskyblue', 'gold', 'lightcoral', 'blueviolet', 'grey', 'lightgreen']
patches, texts = plt.pie(sizes, colors = colors, shadow = True, startangle = 90)

# Equal aspect ratio ensures that pie is drawn as a circle.
plt.axis('equal')
plt.legend(patches, labels, loc = "best")
plt.tight_layout()	
plt.savefig('diaDeLaSemana.png', dpi = 96)