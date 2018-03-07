val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/user/pdg/testhive/nba.csv")

df.select("player").show()

//Renommer la colonne _c0
df=df.withColumnRenamed("_c0","Num")  >> si df est une var et pas val

//Changer type d'une colonne
df.withColumn("Num", df("Num").cast(IntegerType))

//Filtre sur l'age et années différentes de 1990
df.filter(df("age") > 23).filter(df("year") !== 1990).show()

// Moyenne d'age par team
df.select("tm", "age").groupBy("tm").avg("age").show()

//Calcul du nb de ligne par team
df.groupBy("tm").count().sort(desc("count"))

//Requete SQL insight Hive : impact de l'age sur le pourcentage de panier à 2pts
df.createGlobalTempView("nba")
spark.sql("SELECT age AS Age, substr(avg(`2p%`),0,5) AS Moyenne_2pt FROM global_temp.nba where age IS NOT null Group By age").sort(desc("Moyenne_2pt")).show(27)

//Jointure sur 2 DataFrames
df.join(df2, Seq("Num"))
