# TP SPARK - WordCount


* __Définition de la variable d'environnement pour lancer Spark2__
```
$ export SPARK_MAJOR_VERSION=2
$ cd /usr/hdp/current/spark2-client
$ ./bin/spark-shell
```

* __Lecture de la source et conversion en DataSet__
```scala
scala> val data = spark.read.textFile("/tmp/data.txt").as[String]
org.apache.spark.sql.Dataset[String] = [value: string]
```

* __Séparation et regroupement des mots__
```scala
scala> val words = data.flatMap(value => value.split("\\s+"))
org.apache.spark.sql.Dataset[String] = [value: string]

scala> val groupedWords = words.groupByKey(_.toLowerCase)
org.apache.spark.sql.KeyValueGroupedDataset[String,String] = org.apache.spark.sql.KeyValueGroupedDataset@a175266
```

* __Comptage des mots__
```scala
scala> val counts = groupedWords.count()
org.apache.spark.sql.Dataset[(String, Long)] = [value: string, count(1): bigint]
```

* __Affichage du résultat__
```scala
scala>counts.show()

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
|hdfs.audit.logger...|       1|
|yarn.ewma.maxuniq...|       1|
|log4j.appender.nm...|       1|
|              daemon|       1|
|log4j.category.se...|       1|
|log4j.appender.js...|       1|
|log4j.appender.dr...|       1|
|        blockmanager|       1|
|log4j.appender.js...|       1|
|                 set|       4|
+--------------------+--------+
```

* __Sauvegarde dans un fichier__
```scala
scala> counts.rdd.repartition(1).saveAsTextFile("/tmp/yolo2")
```
