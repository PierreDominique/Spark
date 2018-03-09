# TP3 SPARK Streaming - Exemple avec NetCat

## Création du StreamingContext
```scala
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // pas necessaire depuis Spark 1.3

// Création d'un StreamContext local avec deux thread et batch d'interval 1s
// Le master a besoin de 2 cores pour prévenir de l'effet de famine.

scala> val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
scala> val ssc = new StreamingContext(conf, Seconds(1))
```

## Création du DStream se connectant au port localhost:9999
```scala
scala> val lines = ssc.socketTextStream("localhost", 9999)
```

## Séparation de chaque ligne en mots
```scala
scala> val words = lines.flatMap(_.split(" "))
```

## Comptage de chaque dans chaque batch
```scala
scala> val pairs = words.map(word => (word, 1))
scala> val wordCounts = pairs.reduceByKey(_ + _)
```

## Affichage des dix premiers éléments de chaque RDD généré dans ce DStream
```scala
scala> wordCounts.print()
```

## Lancement de la computation et attente de fin
```scala
scala> ssc.start()
scala> ssc.awaitTermination()
```

## Test du comptage en local
* _Terminal 1_ : lancement de NetCat
```scala
$ nc -lk 9999

bonjour bonjour tout le monde
```
* _Terminal 2_ : lancement du script scal de comptage
```scala
$ ./bin/run-example streaming.NetworkWordCount localhost 9999
...
-------------------------------------------
Time: 1357008430000 ms
-------------------------------------------
(bonjour,2)
(tout,1)
(monde,1)
(le,1)
```
