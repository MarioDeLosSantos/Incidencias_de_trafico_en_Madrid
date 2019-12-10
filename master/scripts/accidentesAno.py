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

#Configuracion de Spark
conf = SparkConf().setMaster('local[*]').setAppName('accidentesAno')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

totalAccidentes = 77733.0

#Carga del CSV
table = sqlContext.read.format('csv').options(header = 'true', inferschema = 'true').load('MadridAccidents.csv')

listDate = table.select('FECHA').collect()
listParte = table.select('N PARTE').collect()

previousParte = 000
datePos = 0
years = [0,0,0,0,0,0,0,0]

#Calculo y registro de accidentes por ano (sin repeticion gracias a la comprobacion de los id de partes)
for parte in listParte:
	if(parte == previousParte):
		datePos += 1
	else:
		currentyear = str(listDate[datePos]).split('-')[2][0] + str(listDate[datePos]).split('-')[2][1]
		if(currentyear == '10'):
			years[0] += 1
		elif(currentyear == '12'):
			years[1] += 1
		elif(currentyear == '13'):
			years[2] += 1
		elif(currentyear == '14'):
			years[3] += 1
		elif(currentyear == '15'):
			years[4] += 1
		elif(currentyear == '16'):
			years[5] += 1
		elif(currentyear == '17'):
			years[6] += 1
		elif(currentyear == '18'):
			years[7] += 1
		else:
			print('---------------ERROR----------------')

		datePos += 1

	previousParte = parte

#Calculo de los porcentajes-resultado
i = 0
for x in years:
	years[i] = (float(x)*100.0/totalAccidentes)
	i += 1

#Impresion del resultado en consola
for x in years:
	print(str(x) + '%')

#Grafico

labels ='2010','2012','2013','2014','2015','2016','2017','2018'
sizes = [years[0],years[1],years[2],years[3],years[4],years[5],years[6],years[7]]

colors = ['red', 'green', 'blue', 'orange', 'brown', 'yellow','cyan', 'violet']
patches, texts = plt.pie(sizes,colors=colors, shadow=True, startangle=90)


plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.legend(patches, labels, loc="best")
plt.tight_layout()	
plt.savefig('grafica_accidentesAno.png',dpi=96)