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

#Eliminamos los accidentes duplicados quitando aquellos que tengan la misma fecha, mismo rango horario y mismo lugar
table=table.dropDuplicates(["FECHA","RANGO HORARIO"])
table=table.dropDuplicates(["LUGAR ACCIDENTE"])
	
#Ordenamos y sacamos el porcentaje de los accidentes totales
table=table.groupby("RANGO HORARIO").count().withColumn("%",col("count")*100/(table.select("RANGO HORARIO").count()))

#DIBUJAR LA GRAFICA


tipos=table.select('RANGO HORARIO').collect()
valores=table.select('%').collect()

labels = [tipos[0]['RANGO HORARIO'], tipos[1]['RANGO HORARIO'], tipos[2]['RANGO HORARIO'], tipos[3]['RANGO HORARIO'],tipos[4]['RANGO HORARIO'],tipos[5]['RANGO HORARIO'],
tipos[6]['RANGO HORARIO'],tipos[7]['RANGO HORARIO'],tipos[8]['RANGO HORARIO'],tipos[9]['RANGO HORARIO'],tipos[10]['RANGO HORARIO'], tipos[11]['RANGO HORARIO'], tipos[12]['RANGO HORARIO'], 
tipos[13]['RANGO HORARIO'],tipos[14]['RANGO HORARIO'],tipos[15]['RANGO HORARIO'],tipos[16]['RANGO HORARIO'], tipos[17]['RANGO HORARIO'], tipos[18]['RANGO HORARIO'], tipos[19]['RANGO HORARIO'],
tipos[20]['RANGO HORARIO'],tipos[21]['RANGO HORARIO'],tipos[22]['RANGO HORARIO'],tipos[23]['RANGO HORARIO']]

sizes = [valores[0]['%'], valores[1]['%'], valores[2]['%'], valores[3]['%'],valores[4]['%'],valores[5]['%'],valores[6]['%'],valores[7]['%'],valores[8]['%'],
valores[9]['%'],valores[10]['%'], valores[11]['%'], valores[12]['%'], valores[13]['%'],valores[14]['%'],valores[15]['%'],valores[16]['%'],valores[17]['%'],valores[18]['%'],
valores[19]['%'], valores[20]['%'], valores[21]['%'], valores[22]['%'],valores[23]['%']]

plt.figure(figsize=(1200/96, 800/96), dpi=96)

colors = ['yellowgreen', 'gold', 'lightskyblue', 'lightcoral','black','red','blue','green','yellow','darkorange','darkgoldenrod',
'lawngreen','olive','forestgreen','lime','palegreen','cyan','midnightblue','blueviolet','steelblue','thistle','bisque','chocolate','azure']

patches, texts = plt.pie(sizes,colors=colors, shadow=True, startangle=90)


plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.legend(patches, labels, loc="best")
plt.tight_layout()

plt.savefig('rangoHorario.png',dpi=96)


