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
conf=SparkConf().setMaster('local[*]').setAppName('rangoEdad')
sc=SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#Carga del CSV
table = sqlContext.read.format('csv').options(header = 'true', inferschema = 'true').load('MadridAccidents.csv')

tipoImplicado = table.select('TIPO PERSONA').collect()	#No se tendra en cuenta como implicados a los 'Testigos'
edadList = table.select('Tramo Edad').collect()
edadDebug = table.groupby('Tramo Edad').count().show()	#DEBUG

edadi = 0	#edadList counter
edadResult = [0,0,0,0,0]	#Rangos de edad [0,10) [10,25) [25,65) 65+ Desconocida
edadResultPer = ['0','0','0','0','0']	#Porcentajes
error = False

for tipo in tipoImplicado:
	t = tipo[0]	#tipo de persona implicada

	if(t != 'TESTIGO'):
		edad = str(edadList[edadi][0])

		#Procesamiento del rango de edad del csv al mencionado previamente
		if(edad == 'DE 0 A 5 ANOS'):
			edadResult[0] += 1
		elif(edad == 'DE 6 A 9 ANOS'):
			edadResult[0] += 1
		elif(edad == 'DE 10 A 14 ANOS'):
			edadResult[1] += 1
		elif(edad == 'DE 15 A 17 ANOS'):
			edadResult[1] += 1
		elif(edad == 'DE 18 A 20 ANOS'):
			edadResult[1] += 1
		elif(edad == 'DE 21 A 24 ANOS'):
			edadResult[1] += 1
		elif(edad == 'DE 25 A 29 ANOS'):
			edadResult[2] += 1
		elif(edad == 'DE 30 A 34 ANOS'):
			edadResult[2] += 1
		elif(edad == 'DE 35 A 39 ANOS'):
			edadResult[2] += 1
		elif(edad == 'DE 40 A 44 ANOS'):
			edadResult[2] += 1
		elif(edad == 'DE 45 A 49 ANOS'):
			edadResult[2] += 1
		elif(edad == 'DE 50 A 54 ANOS'):
			edadResult[2] += 1
		elif(edad == 'DE 55 A 59 ANOS'):
			edadResult[2] += 1
		elif(edad == 'DE 60 A 64 ANOS'):
			edadResult[2] += 1
		elif(edad == 'DE 65 A 69 ANOS'):
			edadResult[3] += 1
		elif(edad == 'DE 70 A 74 ANOS'):
			edadResult[3] += 1
		elif(edad == 'DE MAS DE 74 ANOS'):
			edadResult[3] += 1
		elif(edad == 'DESCONOCIDA'):
			edadResult[4] += 1
		else:
			error = True
	else:
		pass
	edadi += 1

#Calculo de porcentajes
tot = edadResult[0] + edadResult[1] + edadResult[2] + edadResult[3] + edadResult[4]

edadResultPer[0] = str(edadResult[0] * 100 / tot)
edadResultPer[1] = str(edadResult[1] * 100 / tot)
edadResultPer[2] = str(edadResult[2] * 100 / tot)
edadResultPer[3] = str(edadResult[3] * 100 / tot)
edadResultPer[4] = str(edadResult[4] * 100 / tot)

#Impresion de resultados en consola
print ('Total accidentes: ' + str(tot))
print('Nino : ' + str(edadResult[0]) + ' / ' + edadResultPer[0] + ' %')
print('Joven : ' + str(edadResult[1]) + ' / ' + edadResultPer[1] + ' %')
print('Adulto : ' + str(edadResult[2]) + ' / ' + edadResultPer[2] + ' %')
print('Anciano : ' + str(edadResult[3]) + ' / ' + edadResultPer[3] + ' %')
print('Desconocido : ' + str(edadResult[4]) + ' / ' + edadResultPer[4] + ' %' + '\n')

if(error):
	print('###Error detectado durante el procesamiento!###')
else:
	print('Completado con exito!' + '\n')

#Grafico

labels ='Nino [0,10)','Joven [10,25)','Adulto [25,65)', 'Anciano +65', 'Desconocido'
sizes = [edadResult[0], edadResult[1], edadResult[2], edadResult[3], edadResult[4]]

colors = ['blue', 'orange', 'cyan', 'brown', 'black']
patches, texts = plt.pie(sizes,colors=colors, shadow=True, startangle=90)


plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.legend(patches, labels, loc="best")
plt.tight_layout()	
plt.savefig('grafica_accidentesRangoEdad.png',dpi=96)