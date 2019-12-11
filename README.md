# Proyecto Cloud

## Índice
1. **El Proyecto**
   - Descripción del problema
   - Necesidad de Big Data
   - Solución
2. **Modelo de datos**
3. **Descripción técnica**
4. **Rendimiento**
   -Gráficos con matplotlib en PySpark
   -Desarrollo del Machine Learning
5. **Implementación avanzada**
   - Gráficos de matplotlib
   - Machine learning
6. **Conclusión**


### 1. **El proyecto**
#### 1.1 Descripción del problema
El objetivo de este proyecto es tratar de disminuir el número de accidentes en cualquier área metropolitana mediante un estudio de incidencias en el tráfico. En concreto hemos llevado a cabo el estudio en la ciudad de Madrid analizando registros de accidentes de tráfico desde 2010.

#### 1.2 Necesidad de Big Data
La cantidad de datos y el número de operaciones necesarias a fin de obtener estadísticas reseñables puede llegar a ser masiva, por lo que se necesita una gran potencia de cómputo. Por otra parte, el estudio puede llegar a realizarse en tiempo real, por lo que también conlleva la necesidad implícita de uso de técnicas Big Data. No obstante, con el dataset usado en este prototipo no se ha requerido una gran potencia de cómputo ni la necesidad de una paralelización reseñable.

#### 1.3 Solución
Para buscar solución al problema planteado hemos usado principalmente dos métodos:

- Estudio de los registros:

El principal objetivo de este proyecto ha sido el estudio analítico de los registros de accidentes usados como dataset. Mediante    técnicas de *data analysis* y estadística hemos sacado una gran cantidad de información de gran relevancia para la toma de medidas adecuadas en el ámbito de la accidentalidad en el tráfico de Madrid.

En un primer momento hemos limpiado y unido los distintos archivos obtenidos del ayuntamiento de Madrid en un solo archivo .csv
sobre el que posteriormente trabajaríamos.
En segundo lugar hemos desarrollado distintos scripts en python que hemos corrido sobre el archivo .csv. Estos tenían como objetivo sacar datos estadísticos sobre los accidentes, desde la relación de accidentes dependiendo del sexo de los implicados a la cantidad de accidentes en los distintos distritos de la ciudad.
Por último y con el soporte de la librería **matplotlib** para facilitar la lectura y el manejo de los datos obtenidos hemos realizado diversos gráficos de apoyo.

- Programa calculador de riesgo:

El segundo objetivo del proyecto ha sido la creación de un pequeño programa en python que calcule el riesgo de realizar un determinado recorrido a partir de una determinado input. (Mas información en **5. Implementación avanzada**)
        

### 2. **Modelo de datos**

![Datos Ayuntamiento](/master/images/datosAyuntamiento.PNG)

Los datos han sido recopilados del Portal de Datos Abiertos del Ayuntamiento de Madrid. Para hacer este estudio nos hemos basado en los accidentes de tráfico en la Ciudad de Madrid registrados por la Policía Municipal, los cuales se hacen cuando hay víctimas o daños al patrimonio. En los datos manejados se incluye un registro por persona implicada en el accidente, y las especificaciones manejadas son:

- Fecha en formato dd/mm/aaaa.
- Rango horario: la hora se establece en intervalos de 1 hora.
- Dia de la Semana: de lunes a domingo.
- Distrito por nombre.
- Lugar del accidente: calle o cruce de calles.
- Número de la calle.
- Número del parte de accidente.
- Condiciones ambientales: granizo, hielo, lluvia, niebla, seco o nieve.
- Estado de la vía: mojada, aceite, barro, grava suelta, hielo o seca y limpia.
- Tipo de accidente: colisión doble, colisión múltiple, choque con objeto fijo, atropello, vuelco, caída de motocicleta, caída ciclomotor, caída de bicicleta, caída de viajero en un bus.
- Tipo de vehículo.
- Tipo de persona: conductor, peatón, testigo o viajero.
- Sexo: hombre, mujer o no especificado.
- Lesividad: Herido leve, herido grave o muerto.
- Tramo de edad de la persona implicada.

Disponemos de 1 dataset que cubre todos los años desde el 2010 hasta 2018(a excepción de 2011), con aproximadamente 226.000 líneas de registros.

### 3. **Descripción técnica**

Conjunto de software total creado:
- 1 archivo .csv dedicado a los scripts de data analysis
- 1 archivo .csv dedicado al script de calculo de riesgo
- 13 scripts en python para el procesamiento del .csv y la obtención de gráficos
- 1 script en python calculador de riesgo
- 13 imágenes .png de gráficos representativos de las conclusiones obtenidas
![Gráficos ejemplo](/master/images/graficosEjemplo.PNG)
![Scripts](/master/images/scripts.PNG)

Todo el proceso, tanto la creación del software como la ejecucción de scripts, lo hemos realizado en ubuntu de manera local en nuestros ordenadores personales.
Para la edición de los scripts usamos *Sublime Text*. Su ejecución la hemos realizado mediante el terminal de Ubuntu *Bash* con el soporte de *Pyspark*. Para su correcto funcionamiento el archivo del dataset (MadridAccidents.csv) se debería situar en el mismo directorio que el script de analisis que se desee ejecutar. Para ejecutar el script unicamente habra que poner "spark-submit (nombre del script)".Tras la ejecución se obtendrá por output en el terminal un resumen del analisis realizado y una imagen .png con el gráfico correspondiente que se guardará en el mismo directorio desde donde se ejecuto el script.

### 4. **Rendimiento**
Dentro de este apartado analizaremos el rendimiento de las dos caras de nuestro proyecto, tanto el data-processing con pyspark como el desarrollo del machine learning.

#### 4.1 **Gráficos con matplotlib en PySpark**
Dado que el coste de crear los gráficos después de procesar los datos es casi despreciable podemos analizar el rendimiento de este proceso unicamente analizando el rendimiento de procesar nuestra gran cantidad de datos.
El rendimiento en el data-processing para la creacion de los gráficos es equivalente a los cores que se le adjunte al proceso, por tanto la escabilidad es realmente alta.Éstos gráficos lo hemos creado localmente en nuestros dispositivos usando todos los cores disponibles en éstos, pero hemos hecho pruebas en Clusters de Amazon paralelizando el proceso haciendo que el tiempo de espera hasta adquirir el resultado se acorte notablemente.

#### 4.2 **Desarrollo del Machine Learning**
El desarrollo del Machine Learning ha sido principalmente implementado localmente por un motivo en particular y es que el único cuello de botella aparece cuando se modifica los datos fuente a procesar, lo que hace que cualquier ordenador de propósito general sea suficiente para el proceso.Por tanto la escalabilidad en unicamente necesaria cuando se cambie los datos a procesar , en caso contrario no es necesario. (Ver el porque en el apartado (Mas información en **5.2 Implementación avanzada**)

