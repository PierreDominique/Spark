# TP SPARK - Data NBA

## Chargement du fichier csv
```scala
scala> var df = spark.read.option("header", "true").option("inferSchema", "true").csv("/user/pdg/testhive/nba.csv")

```

## Renommer une colonne
```scala
scala> df = df.withColumnRenamed("_c0","Num")

```

## Changer le type d'une colonne
```scala
scala> df = df.withColumn("Num", df("Num").cast(IntegerType))

```

## Requêtes
* Afficher la liste des joueurs (limit 10)
```scala
scala> df.select("player").distinct().show(10)
+----------------+
|          player|
+----------------+
|        Ed Mikan|
|       Red Rocha|
|     John Barber|
|      Al Bianchi|
|  Reggie Harding|
|       Bob Allen|
|     Greg Howard|
|Chuckie Williams|
|  Nate Blackwell|
|     Terry Mills|
+----------------+

```

* Moyenne d'age par franchise
```scala
scala> df.select("tm", "age").groupBy("tm").avg("age").show()

```

* Requete SQL - Insight Hive : impact de l'age sur le pourcentage de panier à 2pts
```scala
scala> df.createGlobalTempView("nba")
scala> spark.sql("SELECT age AS Age, substr(avg(`2p%`),0,5) AS Moyenne_2pt FROM global_temp.nba where age IS NOT null Group By age").sort(desc("Moyenne_2pt")).show(27)

```

* Jointure sur 2 DataFrames
```scala
scala> df.join(df2, Seq("Num"))

```



//Filtre sur l'age et années différentes de 1990
df.filter(df("age") > 23).filter(df("year") !== 1990).show()

//Calcul du nb de ligne par team
df.groupBy("tm").count().sort(desc("count"))
