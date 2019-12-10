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
import matplotlib.pyplot as plt

#Configuracion de Spark, usando todos los cores disponibles
conf=SparkConf().setMaster('local[*]').setAppName('rangoHorario')
sc=SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#Carga del CSV
table = sqlContext.read.format('csv').options(header = 'true', inferschema = 'true').load('MadridAccidents.csv')

dl = table.groupby('DISTRITO').count()
amountlist = dl.select('count').collect()
dl = dl.select('DISTRITO').collect()

i = 0
for x in dl:
	print (str(x[0]) + ': ' + str(amountlist[i][0]))
	i += 1


#Grafico

labels = 'Latina', 'Tetuan', 'Salamanca', 'Retiro', 'Moncloa-Aravaca',
'Hortaleza', 'Puente de Vallecas', 'San Blas', 'Villaverde', 'Fuencarral-El Pardo',
'Chamberi', 'Ciudad Lineal', 'Barajas', 'Villa de Vallecas', 'Vicalvaro',
'Carabanchel', 'Centro', 'Chamartin', 'Moratalaz', 'Arganzuela', 'Usera'
sizes = [amountlist[0][0], amountlist[1][0], amountlist[2][0], amountlist[3][0], amountlist[4][0]
, amountlist[5][0], amountlist[6][0], amountlist[7][0], amountlist[8][0], amountlist[9][0]
, amountlist[10][0], amountlist[11][0], amountlist[12][0], amountlist[13][0], amountlist[14][0]
, amountlist[15][0], amountlist[16][0], amountlist[17][0], amountlist[18][0], amountlist[19][0]
, amountlist[20][0]]

colors = ['yellowgreen', 'gold', 'lightskyblue', 'lightcoral','black','red','blue','green','yellow','darkorange','darkgoldenrod',
'lawngreen','olive','forestgreen','lime','palegreen','cyan','midnightblue','blueviolet','steelblue','chocolate']
patches, texts = plt.pie(sizes,colors=colors, shadow=True, startangle=90)


plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.legend(patches, labels, loc="best")
plt.tight_layout()	
plt.savefig('grafica_distrito.png',dpi=96)