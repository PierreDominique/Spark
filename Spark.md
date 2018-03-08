# TP SPARK

export SPARK_MAJOR_VERSION=2
cd /usr/hdp/current/spark2-client
./bin/spark-shell


val data = spark.read.textFile("/tmp/data.txt").as[String]
>> org.apache.spark.sql.Dataset[String] = [value: string]

val words = data.flatMap(value => value.split("\\s+"))
>> org.apache.spark.sql.Dataset[String] = [value: string]

val groupedWords = words.groupByKey(_.toLowerCase)
>> org.apache.spark.sql.KeyValueGroupedDataset[String,String] = org.apache.spark.sql.KeyValueGroupedDataset@a175266

val counts = groupedWords.count()
>> org.apache.spark.sql.Dataset[(String, Long)] = [value: string, count(1): bigint]

counts.rdd.repartition(1).saveAsTextFile("/tmp/yolo2")
>> Sauvegarde dans 1 fichier
