# OctopusSpark - test

## This project focus in take advantage of spark as main tool for manage datas

## This project has three main tentacle in the moment that was created



# 1. The first tentacle is connectios and focus in get data from api, diferent formats and jdbc sources
# 2. The second tentacle focus in injestion in datalake from diferentes sources and formats of data
# 3. The third tentacle is sources that focus in read diferents formats of data and convert in data frame for later analysis



## This project is build in maven with java 1.8.0_242 and scala 2.12.10. The rest of the dependencies are explicited in pom.xml

## For get the project we only need clone the repositorie and define java and scala in our ID(In my case I have used intellij)

## For JDBC we have embebed derby database. The dependency is specified in pom.xml and the proccess for get JDBC conecction in our local is in /SparkOctopus/src/main/java/org/example/sources/data/JavaDerbyConn.java (http://shengwangi.blogspot.com/2015/10/how-to-use-embedded-java-db-derby-in-maven.html)

## Finaly this project has spark.example folder that contains examples of spark that read from diferent formats



### The goal of this proyect is get more tentacles for spark as sparkStreaming for data in streaming and/or cloud for cloud integration

### The other goal of this project is get a jar for compilation anywhere that JVM(1.8.0_242) have been installed 



# This project is alive and will be changes in it
