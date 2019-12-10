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
conf=SparkConf().setMaster('local[*]').setAppName('sexo')
sc=SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#Carga del CSV
table = sqlContext.read.format('csv').options(header = 'true', inferschema = 'true').load('MadridAccidents.csv')

listParte = table.select('N PARTE').collect()
listSex = table.select('SEXO').collect()
X = table.groupby('SEXO').count().show() #DEBUG

previousParte = 000
sexListPos = 0
sexResult = [0,0,0]	#Result counter (H,M,N/A)
error = False

#Calculo de valores de cada sexo implicado en accidentes
#Se comprueba el identificador único de los partes de accidentes para evitar repeticiones
for parte in listParte:
	if(parte == previousParte):
		sexListPos += 1
	else:
		sex = listSex[sexListPos][0]
		if(sex ==  'HOMBRE'):
			sexResult[0]+= 1
		elif(sex ==  'MUJER'):
			sexResult[1]+= 1
		elif(sex ==  'NO ASIGNADO'):
			sexResult[2]+= 1
		else:
			error = True

		sexListPos += 1

	previousParte = parte

print('Accidentes por sexo (Resultado Final): ' + '\n')
print('Hombre: ' + str(sexResult[0]))
print('Mujer: ' + str(sexResult[1]))
print('No Asignado: ' + str(sexResult[2]) + '\n')

if (error):
	print('***SE HANPRODUCIDO ERRORES***')
else:
	print('Proceso completado!')

#Grafico

labels ='Hombres','Mujeres','No asignado'
sizes = [sexResult[0], sexResult[1], sexResult[2]]

colors = ['blue', 'orange', 'violet']
patches, texts = plt.pie(sizes,colors=colors, shadow=True, startangle=90)


plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.legend(patches, labels, loc="best")
plt.tight_layout()	
plt.savefig('grafica_accidentesSexo.png',dpi=96)