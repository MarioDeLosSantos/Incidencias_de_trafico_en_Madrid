#Para la creacion y manejo de arrays en el proceso de la ejecucion
import numpy as np
#Para recoger los datos del csv que vamos a procesar
from pandas.io.parsers import read_csv
#Para coger los argumentos del programa
import sys

#Cargamos los datos que vamos a necesitar
def carga_csv(file_name):
    """carga el fichero csv especificado y lo devuelve en un array de numpy
    """
    valores = read_csv(file_name, header=None).values
 # suponemos que siempre trabajaremos con float
    return valores
 
#Convertidos todos los datos a enteros para poder trabajar con ellos
def convertX(X):
    X[:,0]=convertDistrict(X[:,0])
    X[:,1]=convertDay(X[:,1])
    X[:,2]=convertCar(X[:,2])
    X[:,3]=convertPerson(X[:,3])
    return X

def convertentrada(X):
    X[0]=int(convertSingleDistrict(X[0]))
    X[1]=int(convertSingleDay(X[1]))
    X[2]=int(convertSingleCar(X[2]))
    X[3]=int(convertSinglePerson(X[3]))
    return X

#Calculo de la h
def h(theta,X):
    return np.dot(X,theta[:,None])    

#Calculo del gradiente
def descensoGradiente(X, Y,alpha):
    #Inicilizamos el vector de Theta con O's
    theta=np.zeros(5)
    m=X.shape[0]

    for casos in range(30000):
        for j in range(5):
            cosa=bucle(m,theta,X,Y,j)
            theta[j]=theta[j]-alpha * (1 / (m+1)) * cosa
    return theta

#Sumatorios
def bucle(m,theta,X_norm,Y,j):
    sumatorio=0
    for i in range(m-1):
        sumatorio+=(h(theta,X_norm[i])- Y[i])*X_norm[i,j]
    return sumatorio    

#Calculo del coste
def coste(X,Y,Theta):
    H=np.dot(X,Theta)
    Aux=(H-Y)**2
    return Aux.sum()/(2*len(X))

#Suma los valores de una columna concreta
def sumaValores(Y):
    valor=0
    for i in Y:
        valor+=i
    return valor

def main():
    #Datos
    datos=carga_csv("machineLearning.csv")

    #X de la funcion
    X=np.array([datos[:,0],datos[:,1],datos[:,2],datos[:,3]]).T
    
    #Y de la funcion
    Y=np.array([datos[:,5]]).T

    #Dado que para hacer machine leaerning necesitamos datos numericos, cambiaremos el atributo
    #i-esimo de la matriz X por un valor entero y justo despues normalizamos los valores para poder 
    #trabajar con ellos
    X=convertX(X)
    X_norm,mu,sigma=normaliza(X)

    # #Una vez tenemos convertidos las graficas procederos al machine learning
    # ###  REGRESION LINEAL  ###
    # #Anadimos unos a la columna de X para poder operara con ellas
    XwithOnes=np.hstack((np.ones(shape=(X_norm.shape[0],1)),X_norm))

    # #Sacamos el ThetaOptimo que hara minimo la funcion de coste
    # gradiente_=descensoGradiente(XwithOnes,Y,0.0001)
    # print(gradiente_)

    ThetaOptimo=np.array([ 8.22286797e-07 ,-4.72866099e-08 ,-4.46854796e-08 , 2.04102162e-07 ,-5.48385083e-08])

    #Una vez tenemos el thetaMinimo,predecimos un nuevo dato, normalizandolos primero
    datos_=np.array([sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4]])
    NuevoDato=convertentrada(datos_)
    x0=(int(NuevoDato[0])-mu[0])/sigma[0]  
    x1=(int(NuevoDato[1])-mu[1])/sigma[1]
    x2=(int(NuevoDato[2])-mu[2])/sigma[2]
    x3=(int(NuevoDato[3])-mu[3])/sigma[3]

    prediccionDG=np.dot(np.array([1,x0,x1,x2,x3]),ThetaOptimo.T)
    print(prediccionDG)

    #Una vez tenemos el porcentaje del nuevo caso calculado, veremos 
    #su peligrosidad relativa respecto a la media de accidentes segun
    #las columnas de nuestros datos

    #Media de Accidentes
    mediaAccidentes=sumaValores(Y)/X.shape[0]

    #Porcentaje que sacaremos por pantalla
    porcentajeRelativo=((prediccionDG/mediaAccidentes)*100)-100

    #Calculamos si esa ruta es mas o menos peligrosa que la media de rutas
    if(porcentajeRelativo<0):
        print("Esa ruta es un {:.2f}% mas segura que la media de accidentes con esas caracteristicas".format(abs(porcentajeRelativo[0])))
    else: print("Esa ruta es un {:.2f}% mas peligrosa que la media de accidentes con esas caracteristicas".format(porcentajeRelativo[0]))
   

def normaliza(X):
    #Devuelve el vector normalzado ademas de un vector con la media y otro con la desviacion de cada
    #caso de entrenamiento
    mu=np.array([])
    sigma=np.array([])

    #Media y desviacion estandar
    for i in range(X.shape[1]):
        mu=np.append(mu,[np.mean(X[:,i])])
        sigma=np.append(sigma,[np.std(X[:,i])])

    #Nomalizamos la matriz
    for cols in range(X.shape[1]):
       X[:, cols] = (X[:,cols] - mu[cols]) / sigma[cols] 
    return (X,mu,sigma)

#Convertimos todos los tipos de personas a enteros
def convertPerson(X):
    for i in range(len(X)):
        if X[i]=='CONDUCTOR':
            X[i]=0
        elif X[i]=='VIAJERO':
            X[i]=1
        elif X[i]=='TESTIGO':
            X[i]=2
        elif X[i]=='PEATON':
            X[i]=3
        else: 
            #En caso de que haya alguna errata desconocida, ponemos esa errata como TESTIGO ya que es donde menos 
            # accidentes se producen para que no modifique mucho el porcentaje
            X[i]=2
        X[i]=float(X[i])
    return X

#Convertimos todos los dias a enteros        
def convertDay(X):
    for i in range(len(X)):
        if X[i]=='LUNES':
            X[i]=0
        elif X[i]=='MARTES':
            X[i]=1
        elif X[i]=='MIERCOLES':
            X[i]=2
        elif X[i]=='JUEVES':
            X[i]=3
        elif X[i]=='VIERNES':
            X[i]=4
        elif X[i]=='SABADO':
            X[i]=5
        elif X[i]=='DOMINGO':
            X[i]=6    
        else: 
            #En caso de que haya alguna errata desconocida, ponemos esa errata como DOMINGO ya que es donde menos 
            # accidentes se producen para que no modifique mucho el porcentaje
            X[i]=6
        X[i]=float(X[i])    
    return X

#Convertimos todos los tipos de coches a enteros    
def convertCar(X):
    for i in range(len(X)):
        if X[i]=='AUTOBUS-AUTOCAR':
            X[i]=0
        elif X[i]=='CICLOMOTOR':
            X[i]=1
        elif X[i]=='NO ASIGNADO':
            X[i]=2
        elif X[i]=='BICICLETA':
            X[i]=3
        elif X[i]=='AMBULANCIA':
            X[i]=4
        elif X[i]=='CAMION':
            X[i]=5
        elif X[i]=='AUTO-TAXI':
            X[i]=6
        elif X[i]=='TURISMO':
            X[i]=7
        elif X[i]=='VARIOS':
            X[i]=8
        elif X[i]=='VEH.3 RUEDAS':
            X[i]=9
        elif X[i]=='FURGONETA':
            X[i]=10
        elif X[i]=='MOTOCICLETA':
            X[i]=11
        else:
            X[i]=9
            #En caso de que haya alguna errata desconocida, ponemos esa errata como VEH.3 RUEDAS ya que es donde menos 
            # accidentes se producen para que no modifique mucho el porcentaje
        X[i]=float(X[i])
    return X

    #Convertimos todos los distritos a enteros   
def convertDistrict(X):
    for i in range(len(X)):
        if X[i]=='LATINA':
            X[i]=0
        elif X[i]=='TETUAN':
            X[i]=1
        elif X[i]=='SALAMANCA':
            X[i]=2
        elif X[i]=='RETIRO':
            X[i]=3
        elif X[i]=='MONCLOA-ARAVACA':
            X[i]=4
        elif X[i]=='HORTALEZA':
            X[i]=5
        elif X[i]=='PUENTE DE VALLECAS':
            X[i]=6
        elif X[i]=='SANBLAS':
            X[i]=7
        elif X[i]=='VILLAVERDE':
            X[i]=8
        elif X[i]=='FUENCARRAL':
            X[i]=9
        elif X[i]=='CHAMBERI':
            X[i]=10
        if X[i]=='CIUDAD LINEAL':
            X[i]=11
        elif X[i]=='BARAJAS':
            X[i]=12
        elif X[i]=='VILLA DE VALLECAS':
            X[i]=13
        elif X[i]=='VICALVARO':
            X[i]=14
        elif X[i]=='CARABANCHEL':
            X[i]=15
        elif X[i]=='CENTRO':
            X[i]=16
        elif X[i]=='CHAMARTIN':
            X[i]=17
        elif X[i]=='MORATALAZ':
            X[i]=18
        elif X[i]=='ARGANZUELA':
            X[i]=19
        elif X[i]=='USERA':
            X[i]=20
        #En caso de que haya alguna errata desconocida, ponemos esa errata como SANBLAS ya que es donde menos 
        # accidentes se producen para que no modifique mucho el porcentaje   
        else: X[i]=7
        X[i]=float(X[i])
    return X


### DEBIDO A QUE LOS METODOS DE CONVERSION ANTERIORES SON UNICAMENTE PARA GRANDES LISTAS , NO LOS PODEMOS
### REUTILIZAR PARA CONVERTIR EL NUEVO CASO A PROCESAR, Y POR ELLO USAREMOS ESTOS


#Convertimos todos los dias a enteros        
def convertSingleDay(X):
    if X=='LUNES':
        return 0
    elif X=='MARTES':
        return 1
    elif X=='MIERCOLES':
        return 2
    elif X=='JUEVES':
        return 3
    elif X=='VIERNES':
        return 4
    elif X=='SABADO':
        return 5
    elif X=='DOMINGO':
        return 6    
    else: 
        #En caso de que haya alguna errata desconocida, ponemos esa errata como DOMINGO ya que es donde menos 
        # accidentes se producen para que no modifique mucho el porcentaje
        return 6

#Convertimos todos los tipos de coches a enteros    
def convertSingleCar(X):
    if X=='AUTOBUS-AUTOCAR':
        return 0
    elif X=='CICLOMOTOR':
        return 1
    elif X=='NO ASIGNADO':
        return 2
    elif X=='BICICLETA':
        return 3
    elif X=='AMBULANCIA':
        return 4
    elif X=='CAMION':
        return 5
    elif X=='AUTO-TAXI':
        return 6
    elif X=='TURISMO':
        return 7
    elif X=='VARIOS':
        return 8
    elif X=='VEH.3 RUEDAS':
        return 9
    elif X=='FURGONETA':
        return 10
    elif X=='MOTOCICLETA':
        return 11
    else:
        return 9
        #En caso de que haya alguna errata desconocida, ponemos esa errata como VEH.3 RUEDAS ya que es donde menos 
        # accidentes se producen para que no modifique mucho el porcentaje

#Convertimos todos los distritos a enteros   
def convertSingleDistrict(X):
    if X=='LATINA':
        return 0
    elif X=='TETUAN':
        return 1
    elif X=='SALAMANCA':
        return 2
    elif X=='RETIRO':
        return 3
    elif X=='MONCLOA-ARAVACA':
        return 4
    elif X=='HORTALEZA':
        return 5
    elif X=='PUENTE DE VALLECAS':
        return 6
    elif X=='SANBLAS':
        return 7
    elif X=='VILLAVERDE':
        return 8
    elif X=='FUENCARRAL':
        return 9
    elif X=='CHAMBERI':
        return 10
    if X=='CIUDAD LINEAL':
        return 11
    elif X=='BARAJAS':
        return 12
    elif X=='VILLA DE VALLECAS':
        return 13
    elif X=='VICALVARO':
        return 14
    elif X=='CARABANCHEL':
        return 15
    elif X=='CENTRO':
        return 16
    elif X=='CHAMARTIN':
        return 17
    elif X=='MORATALAZ':
        return 18
    elif X=='ARGANZUELA':
        return 19
    elif X=='USERA':
        return 20
    #En caso de que haya alguna errata desconocida, ponemos esa errata como SANBLAS ya que es donde menos 
    # accidentes se producen para que no modifique mucho el porcentaje   
    else: return 7  

def convertSinglePerson(X):
    if X=='CONDUCTOR':
        return 0
    elif X=='VIAJERO':
        return 1
    elif X=='TESTIGO':
        return 2
    elif X=='PEATON':
        return 3
    else: 
        #En caso de que haya alguna errata desconocida, ponemos esa errata como TESTIGO ya que es donde menos 
        # accidentes se producen para que no modifique mucho el porcentaje
        return 2


main()
