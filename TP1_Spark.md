# TP1 SPARK - WordCount


## Définition de la variable d'environnement pour lancer Spark2 (+ copie du fichier dans hdfs)
```
$ sudo su spark
$ export SPARK_MAJOR_VERSION=2
$ cd /usr/hdp/current/spark2-client
$ hdfs dfs -copyFromLocal /etc/hadoop/conf/log4j.properties /tmp/data.txt
$ ./bin/spark-shell
```

## Lecture de la source et conversion en DataSet
```scala
scala> val data = spark.read.textFile("/tmp/data.txt").as[String]
org.apache.spark.sql.Dataset[String] = [value: string]
```

## Séparation et regroupement des mots
```scala
scala> val words = data.flatMap(value => value.split("\\s+"))
org.apache.spark.sql.Dataset[String] = [value: string]

scala> val groupedWords = words.groupByKey(_.toLowerCase)
org.apache.spark.sql.KeyValueGroupedDataset[String,String] = org.apache.spark.sql.KeyValueGroupedDataset@a175266
```

## Comptage des mots
```scala
scala> val counts = groupedWords.count()
org.apache.spark.sql.Dataset[(String, Long)] = [value: string, count(1): bigint]
```

## Affichage du résultat
```scala
scala> counts.show(10)

+--------------------+--------+
|               value|count(1)|
+--------------------+--------+
|                some|       1|
|hadoop.security.l...|       1|
|log4j.rootlogger=...|       1|
|log4j.appender.nn...|       1|
|log4j.appender.tl...|       1|
|hadoop.security.l...|       1|
|            license,|       1|
|                 two|       1|
|             counter|       1|
|log4j.appender.dr...|       1|
+--------------------+--------+
```

## Sauvegarde dans un fichier
```scala
scala> counts.rdd.repartition(1).saveAsTextFile("/tmp/yolo2")
```
