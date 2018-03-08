# TP SPARK - Data NBA

## Chargement du fichier csv
```scala
scala> var df = spark.read.option("header", "true").option("inferSchema", "true").csv("/user/pdg/testhive/nba.csv")
```

## Affichage du Schéma de la DataFrame
```scala
scala> scala df.printSchema()
```
```scala
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
scala> spark.sql("SELECT age AS Age, substr(avg(`2p%`),0,5) AS Moyenne_2pt, substr(avg(`3p%`),0,5) AS Moyenne_3pt, substr(avg(`ast%`),0,5) AS Moyenne_Passe, substr(avg(`trb%`),0,5) AS Moyenne_Rebond FROM global_temp.nba where age IS NOT null Group By age").sort("age").show(27)

+---+-----------+-----------+-------------+--------------+
|Age|Moyenne_2pt|Moyenne_3pt|Moyenne_Passe|Moyenne_Rebond|
+---+-----------+-----------+-------------+--------------+
| 18|      0.429|      0.191|        9.192|         11.37|
| 19|      0.449|      0.245|        10.11|         10.88|
| 20|      0.457|      0.259|        11.59|         11.00|
| 21|      0.452|      0.236|        12.22|         10.64|
| 22|      0.435|      0.224|        12.17|         10.00|
| 23|      0.438|      0.230|        12.22|         10.00|
| 24|      0.439|      0.236|        12.63|         9.981|
| 25|      0.447|      0.244|        13.20|         10.14|
| 26|      0.447|      0.242|        13.23|         9.923|
| 27|      0.447|      0.247|        13.43|         10.00|
| 28|      0.453|      0.257|        13.28|         9.834|
| 29|      0.451|      0.265|        13.71|         9.635|
| 30|      0.450|      0.263|        13.15|         9.794|
| 31|      0.447|      0.260|        13.57|         9.511|
| 32|      0.444|      0.273|        13.66|         9.721|
| 33|      0.439|      0.265|        13.84|         9.615|
| 34|      0.441|      0.274|        13.79|         9.493|
| 35|      0.450|      0.286|        13.28|         10.03|
| 36|      0.451|      0.253|        13.27|         10.14|
| 37|      0.436|      0.275|        12.93|         10.48|
| 38|      0.446|      0.251|        13.14|         10.96|
| 39|      0.456|      0.258|        11.89|         10.54|
| 40|      0.460|      0.305|        9.975|         11.14|
| 41|      0.449|        0.0|         9.96|         14.77|
| 42|      0.427|        0.0|        2.699|         16.59|
| 43|       0.49|       null|          8.2|          12.4|
| 44|      0.385|       null|          3.6|          11.2|
+---+-----------+-----------+-------------+--------------+

```

* Joueurs de plus de 35 ans avec les meilleures stats
```scala
scala> df.filter(df("age") > 35).filter($"2p%" > 0.6 && $"2p%".notEqual(1)).select($"Player".alias("Joueur"),$"2p%").groupBy("Joueur").agg(substring(avg("2p%"),0,4).alias("% Panier 2pts")).sort(desc("% Panier 2pts")).show()

+-----------------+-------------+
|           Joueur|% Panier 2pts|
+-----------------+-------------+
|Wilt Chamberlain*|         0.72|
|    Charles Jones|          0.7|
|       Dale Ellis|         0.66|
|      Dana Barros|         0.66|
|        Bo Outlaw|         0.66|
|     Andre Miller|         0.64|
|Shaquille O'Neal*|         0.63|
|    Nick Collison|         0.63|
|   Artis Gilmore*|         0.61|
|    Rasual Butler|         0.61|
|   Chris Andersen|         0.60|
|      Brent Barry|         0.60|
+-----------------+-------------+

```

* Jointure sur 2 DataFrames
```scala
scala> df.join(df2, Seq("Num"))

```
