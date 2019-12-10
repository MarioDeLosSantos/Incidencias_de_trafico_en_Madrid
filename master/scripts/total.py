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

from pyspark import SparkConf, SparkContext, SQLContext
import string
import pyspark
import sys
import matplotlib.pyplot as plt

#Configuracion de Spark
conf = SparkConf().setMaster('local[*]').setAppName('total')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

#2.500.000 - Valor de desplazamientos diarios en la Comunidad de Madrid
despDaily = 2500000.0

#Carga del CSV
table = sqlContext.read.format('csv').options(header = 'true', inferschema = 'true').load('MadridAccidents.csv')

#Numero total de accidentes sin repeticion
nAccTotal=float((table.groupby('N PARTE').count()).count())
#Calculo de accidentes totales y porcentaje de accidentes durante los 8 anos
porcentajeAccidentes = round((nAccTotal * 100.0 / (despDaily * 2918.0)), 4)
porcentajeNoAccidentes = (100.0 - porcentajeAccidentes)
print('Porcentaje de accidentes: '+ str(porcentajeAccidentes)+ '%')
print('Porcentaje de NO accidentes: '+ str(porcentajeNoAccidentes)+ '%')
print('N de accidentes: ' + str(int(nAccTotal)))

#Dibujamos y guardamos la grafica

labels ='Desplazamientos accidentados','Desplazamientos no accidentados'
#Para facilitar el entendimiento de los datos en la grafica, estos se han modifcado
#De manera que el area de desplazamientos accidentados se puede llegar a apreciar
sizes = [1, 99]
#sizes = [porcentajeAccidentes, porcentajeNoAccidentes]

colors = ['red', 'green']
patches, texts = plt.pie(sizes,colors=colors, shadow=True, startangle=90)


plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.legend(patches, labels, loc="best")
plt.tight_layout()
plt.savefig('grafica_total.png')