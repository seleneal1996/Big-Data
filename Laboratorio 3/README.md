
<div align="center">
  <img height="150" src="https://camo.githubusercontent.com/62da68eb62b1e5f175f7d1f0191dd89a653d7908feb22d37d4a0ab07365d6791/68747470733a2f2f6d656469612e67697068792e636f6d2f6d656469612f4d3967624264396e6244724f5475314d71782f67697068792e676966"  />
</div>

###

# LABORATORIO 3: HADOOP

El siguiente laboratorio se dividio en 3 tareas:
- Instalación y configuración del cluster
- Índice invertido  
- Seguridad

## SIMULACIÓN DEL CLUSTER

La simulación realizada es con docker donde se ejecutara 4 nodos Arquitectura de Hadoop

- Almacenamiento distribuido: HDFS
- Planificación de tareas y negociación de recursos: YARN
- Procesamiento distribuido: MapReduce

Usaremos contenedores Docker como máquinas virtuales con bajo overhead

### Instalación base de Hadoop con Docker

- Iniciar el Docker Desktop
- Abre un terminal en esa carpeta y construye la imagen ejecutando
```bash
  $docker build -t hadoop-base-image 
```
- Una vez construida, puedes verla en el Docker Desktop o ejecutando:

```bash
  $docker image ls
```
### Creación de la imagen para el NameNode
- Examina también los ficheros dentro de la carpeta
```bash
  $Hadoop_cluster/NameNode/Config-files
```
- Abrir un terminal en la carpeta NameNode/ y construye la imagen del NameNode ejecutando:
```bash
  $docker build -t namenode-image 
```
- Una vez construida, puedes verla en el Docker Desktop o ejecutando:
docker image ls

### Ejecución del servidor NameNode
- Crear la red que conectará los contenedores ejecutando:
```bash
  $docker network create hadoop-net
```
- Se puede ver la red creada con: 

```bash
  $docker network inspect hadoop-net
```
- Iniciar un contenedor corriendo el servicio NameNode con:

```bash
  $docker container run --rm --init --detach --name namenode --network=hadoop-net --hostname namenode -p 9870:9870 namenode-image
```

### Creación de la imagen para el ResourceManager

- Construir la imagen del ResourceManager ejecutando:

```bash
  $docker build -t resourcemanager-image 
```
- Una vez construida, puedes verla en el Docker Desktop o ejecutando:
```bash
  $docker image ls
```
### Ejecución del servidor ResourceManager

- Inicia un contenedor corriendo el servicio ResourceManager con:

```bash
$docker container run --rm --init --detach --name resourcemanager
--network=hadoop-net --hostname resourcemanager -p 8088:8088
resourcemanager-image
```
- Accede al interfaz web del ResourceManager en http://localhost:8088

### Ejecución de los DataNodes/NodeManagers

- Iniciar cuatro contenedores corriendo los servicios DataNode/NodeManager con
```bash
$docker container run --rm --init --detach --name dnnm1 --network=hadoop-net --hostname dnnm1 dnnm-image
$docker container run --rm --init --detach --name dnnm2 --network=hadoop-net --hostname dnnm2 dnnm-image
$docker container run --rm --init --detach --name dnnm3 --network=hadoop-net --hostname dnnm3 dnnm-image
```
Acceder a los interfaces web del NameNode y del ResourceManager y comprobar que se han registrado los 4 workers

## ÍNDICE INVERTIDO DE BIGRAMAS
El objetivo de esta tarea es generar un índice invertido que mapee cada bigrama a una lista de documentos donde aparece junto con la frecuencia de aparición en cada documento. Este enfoque utiliza el paradigma MapReduce para procesar eficientemente grandes conjuntos de datos distribuidos.

### Crear directorios en HDFS
```bash
$hdfs dfs -mkdir /user/hduser             
$hdfs dfs -mkdir /user/hduser/lab02   
$hdfs dfs -ls /user/hduser/	               
```
### Ejecutar la primera tarea
En tu usuario hadoop
```bash
export JAVA_HOME=/usr/java/latest
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
```
En tu terminal
```bash
$start-dfs.sh
$start-yarn.sh
```
Para enviar el trabajo de Hadoop, la implementación de MadReduce debe empaquetarse como un archivo jar. Para hacerlo, se copia el archivo InvertedIndex.java de este proyecto en la carpeta raíz de distribución de Hadoop y, mientras aún está allí, ejecuta los siguientes comandos para compilar InvertedIndex.java y crear un archivo jar.
```bash
$ hadoop com.sun.tools.javac.Main InvertedIndex.java
$ jar cf invertedindex.jar InvertedIndex*.class
```
### Generar resultados
Usa el siguiente comando:
```bash
$hadoop jar invertedindex.jar InvertedIndex /user/hduser/lab02/fulldata /user/hduser/lab02/output_fulldata
```

## Integrantes
- Barrios Cornejo, Selene
- Fernandez Mamani, Brayan Gino
- Ccari Quispe, Jose









