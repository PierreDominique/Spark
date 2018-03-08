# TP SPARK - WordCount

export SPARK_MAJOR_VERSION=2
cd /usr/hdp/current/spark2-client
./bin/spark-shell


scala> val data = spark.read.textFile("/tmp/data.txt").as[String]
>> org.apache.spark.sql.Dataset[String] = [value: string]

scala> val words = data.flatMap(value => value.split("\\s+"))
>> org.apache.spark.sql.Dataset[String] = [value: string]

scala> val groupedWords = words.groupByKey(_.toLowerCase)
>> org.apache.spark.sql.KeyValueGroupedDataset[String,String] = org.apache.spark.sql.KeyValueGroupedDataset@a175266

scala> val counts = groupedWords.count()
>> org.apache.spark.sql.Dataset[(String, Long)] = [value: string, count(1): bigint]


## Sauvegarde dans 1 fichier
scala> counts.rdd.repartition(1).saveAsTextFile("/tmp/yolo2")
