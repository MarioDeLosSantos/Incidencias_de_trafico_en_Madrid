# Proyecto Cloud

## Índice
1. **El Proyecto**
   - Descripción del problema
   - Necesidad de Big Data
   - Solución
2. **Modelo de datos**
3. **Descripción técnica**
4. **Rendimiento**
   - Gráficos con matplotlib en PySpark
   - Desarrollo del Machine Learning
5. **Implementación avanzada**
   - Implementacion de los gráficos con Matplotlib
   - Implementacion del Machine learning
6. **Conclusión**


### 1. **El proyecto**
#### 1.1 Descripción del problema
El objetivo de este proyecto es tratar de disminuir el número de accidentes en cualquier área metropolitana mediante un estudio de incidencias en el tráfico. En concreto hemos llevado a cabo el estudio en la ciudad de Madrid analizando registros de accidentes de tráfico entre los años 2010-2018.

#### 1.2 Necesidad de Big Data
La cantidad de datos y el número de operaciones necesarias a fin de obtener estadísticas reseñables puede llegar a ser masiva, por lo que se necesita una gran potencia de cómputo. Por otra parte, el estudio puede llegar a realizarse en tiempo real, por lo que también conlleva la necesidad implícita de uso de técnicas Big Data. No obstante, con el dataset usado en este prototipo no se ha requerido una gran potencia de cómputo ni la necesidad del uso de la paralelización.

#### 1.3 Solución
Para buscar solución al problema planteado hemos usado principalmente dos métodos:

- Estudio de los registros:

El principal objetivo de este proyecto ha sido el estudio analítico de los registros de accidentes usados como dataset. Mediante    técnicas de *data analysis* y estadística hemos sacado una gran cantidad de información de gran relevancia para la toma de medidas adecuadas en el ámbito de la accidentalidad en el tráfico de Madrid.

En un primer momento hemos limpiado y unido los distintos archivos obtenidos del ayuntamiento de Madrid en un solo archivo .csv
sobre el que posteriormente trabajaríamos.
En segundo lugar hemos desarrollado distintos scripts en python que hemos corrido sobre el archivo .csv. Estos tenían como objetivo sacar datos estadísticos sobre los accidentes, desde la relación de accidentes dependiendo del sexo de los implicados a la cantidad de accidentes en los distintos distritos de la ciudad.
Por último y con el soporte de la librería **matplotlib** para facilitar la lectura y el manejo de los datos obtenidos hemos realizado diversos gráficos de apoyo.

- Programa calculador de riesgo:

El segundo objetivo del proyecto ha sido la creación de un pequeño programa en python que calcule el riesgo de realizar un determinado recorrido a partir de un determinado input. (Mas información en **5.2 Implementacion del Machine Learning**)
        

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

En cuanto al machine learning se ha de ejecutar el script "MachineLearning.py" seguido de 4 argumentos que seran:
- Distrito 
- Dia de la semana 
- tipo de vehiculo
- tipo de persona afectada
Cualquiera de los datos introducidos tienen que existir dentro del dataset previamente procesado.

### 4. **Rendimiento**
Dentro de este apartado analizaremos el rendimiento de las dos caras de nuestro proyecto, tanto el data-processing con pyspark como el desarrollo del machine learning.

#### 4.1 **Gráficos con matplotlib en PySpark**
Dado que el coste de crear los gráficos después de procesar los datos es casi despreciable podemos analizar el rendimiento de este proceso unicamente analizando el rendimiento de procesar nuestra gran cantidad de datos.
El rendimiento en el data-processing para la creacion de los gráficos es equivalente a los cores que se le adjunte al proceso, por tanto la escabilidad es realmente alta.Éstos gráficos lo hemos creado localmente en nuestros dispositivos usando todos los cores disponibles en éstos, pero hemos hecho pruebas en Clusters de Amazon paralelizando el proceso haciendo que el tiempo de espera hasta adquirir el resultado se acorte notablemente.

#### 4.2 **Desarrollo del Machine Learning**
El desarrollo del Machine Learning ha sido principalmente implementado localmente por un motivo en particular y es que el único cuello de botella aparece cuando se modifica los datos fuente a procesar, lo que hace que cualquier ordenador de propósito general sea suficiente para el proceso.Por tanto la escalabilidad en unicamente necesaria cuando se cambie los datos a procesar , en caso contrario no es necesario. (Ver el porque en el apartado (Mas información en **5.2 Implementación del Machine Learning**)

### 5. **Implementación avanzada**
Dentro de este apartado analizaremos los aspectos mas técnicos del proyecto

#### 5.1 **Implementación de los gráficos con Matplotlib**
Dentro de todas los tipos de gráficas posibles hemos escogido las vistas anteriormente por un motivo en concreto, y es que algunas probabilidades son tan bajas que ponerlas en la gráfica quedan mal, por ello unicamente hemos optado por poner los tamaños relativos al total así como una leyenda para identificar cada una de las probabilidades.
Las formas de asignar el tamaño a cada etiqueta viene dado de una forma similar al siguiente codigo:

![Ejemplo Codigo](/master/images/ejemploPintado.jpg)

#### 5.2 **Implementación del Machine Learning**
La idea de la parte del uso de Machine Learning en el proyecto es la predicción de la probabilidad de tener un accidente dado un nuevo caso que no se encuentre en nuestro dataset.El dataset consta con muchas columnas informativas pero hemos decidido centrarnos en aquellas que creemos que son las mas relevantes que son las siguientes:
- DISTRITO->Distrito de Madrid al que queremos dirigirnos.
- DIA SEMANA->Dia en el que procederemos al viaje.
- Tipo Vehiculo->Tipo de vehículo que usaremos en el viaje.
- TIPO PERSONA->Tipo de persona implicada en el accidente,puede ser (Conductor,Peaton,Testigo o Viajero).

Debido a que nuestro objetivo es predecir una nueva probabilidad (Y de la funcion) para un nuevo caso (X de la funcion), optaremos por usar regresión lineal no regularizada.
Primero implementaremos tanto la función de coste como la función del gradiente.Debido al tamaño de nuestros datos no podremos usar funciones externas para obtener el theta óptimo al instante, por lo que tendremos que implementarnos el descenso de gradiente a mano.
Este proceso puede llegar a ser muy costoso tanto en tiempo como en potencia de computo (dependiendo la precision que queramos obtener) .En este caso en concreto estuvimos aplicando el descenso de gradiente durante aproximadamente 95 minutos en un ordenador local,y se tendrá que repetir este proceso cada vez que el dataset a procesar haya sido modificado, por lo que está claro que tiene una necesidad inmensa del uso de paralelización.

Para nuestro dataset en concreto y con las columnas elegidas anteriormente, este es el theta óptimo:
- ([ 8.22286797e-07 ,-4.72866099e-08 ,-4.46854796e-08 , 2.04102162e-07 ,-5.48385083e-08])

Una vez calculado el theta óptimo, calcular nuevas probabilidades es un proceso trivial y practicamente inmediato que podemos obtener de la siguiente maneras con la ayuda de los arrays de numpy:

- np.dot(X,ThetaOptimo.T) , siendo X los 4 nuevos datos que queramos someter a la regresión linea.
El resultado de esta operación sera la probabilidad de tener un accidente con las caracteristicas expresadas en la nueva X.

Aquí vemos unos cuantos ejemplos del uso de este script el cual imprime la probabilidad de tener un accidente relativa a la media de las probabilidades de tener un accidente con las 4 columnas escogidas anteriormente como características.

![Ejemplo Ejecucion](/master/images/EjemploEjecucion.png)

Es importante hacer saber que el dia donde se producen mas accidentes es el miercoles y el que menos el domingo, el tipo de coche con el que se producen mas accidentes es el turismo, el tipo de persona mas implicada es el condcutor y uno de los que menos es el peaton, y unos de los distritos donde se producen mas accidentes es en Salamanca. Siguiendo este esquema vemos que las probabilidades que nos imprime el programa concuerdan perfectamente.

### 6. **Conclusión**
Dado que nuestro proyecto se dividía en dos facetas siendo una de ellas informar a las competencias locales con graficas que reflejasen los accidentes según un cierto criterio, y la otra crear un programa que hiciese predicciones de cual es la probabilidad de tener un accidente dado un nuevo recorrido con el uso de estrategias de machine learning, podemos decir que hemos conseguido ampliamente el objetivo ya que disponemos de 13 graficas informativas y un programa que crear predicciones muy precisas.
Cabe destacar que el uso de técnicas de Big Data son imprescindibles para las dos facetas de nuestro proyecto. La primera por el tratamiento de datos tan masivos y la segunda por el numero de operaciones matematicas que hay hacer.

Ha sido un gran proyecto para aprender y mejorar herramientas como PySpark,herramientas de machine learning, manejo de Clusters en AWS perfeccionamiento del uso del lenguaje de Python.
En un futuro podríamos añadir mas tipos de gráficas y usar aquellas que sean mas afines a los datos obtenidos y por otra parte podríamos hacer que el machine learning funcionara con cualquier tipo de columna del dataset escogido.

