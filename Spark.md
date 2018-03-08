# TP SPARK - Data NBA

## Chargement du fichier csv
```scala
scala> var df = spark.read.option("header", "true").option("inferSchema", "true").csv("/user/pdg/testhive/nba.csv")
```

## Affichage du Schéma de la DataFrame
```scala
scala> df.printSchema()
 
|-- _c0: double (nullable = true)
|-- Year: integer (nullable = true)
|-- Player: string (nullable = true)
|-- Pos: string (nullable = true)
|-- Age: integer (nullable = true)
|-- Tm: string (nullable = true)
|-- G: integer (nullable = true)
|-- GS: integer (nullable = true)
|-- MP: integer (nullable = true)
|-- PER: double (nullable = true)

...

```

## Renommer une colonne
```scala
scala> df = df.withColumnRenamed("_c0","Num")

|-- Num: double (nullable = true)

```


## Changer le type d'une colonne
```scala
scala> df = df.withColumn("Num", df("Num").cast(IntegerType))

|-- Num: integer (nullable = true)

```

## Requêtes
* Afficher la liste des joueurs (limit 10)
```scala
scala> df.select($"player".alias("Joueur")).distinct().show(10)
+----------------+
|          Joueur|
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
scala> df.filter(df("tm").isNotNull).select($"tm".alias("Franchise"), $"age").groupBy("Franchise").agg(substring(avg("age"),0,5).alias("Moyenne d'age")).sort(asc("Moyenne d'age")).show(10)

+---------+-------------+
|Franchise|Moyenne d'age|
+---------+-------------+
|      CHS|        24.30|
|      CHP|         25.0|
|      DNN|        25.06|
|      INO|        25.21|
|      CHZ|        25.23|
|      SDR|        25.27|
|      MNL|        25.46|
|      STB|        25.57|
|      KCK|        25.68|
|      CIN|        25.70|
+---------+-------------+

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
