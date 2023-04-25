//Start Spark REPL
spark-shell --deploy-mode client

//Scala script to create first RDD using Spark Context
val EMS_RDD = sc.textFile("ems_incident_dispatch_data.csv")

//Remove headers so not evaluated:
val emsHead = EMS_RDD.first()
val emsNoHeader = EMS_RDD.filter(line => !line.equals(emsHead))

//COUNT THE NUMBER OF RECORDS
emsNoHeader.count()

//EXPLORE YOUR DATA
//See how many partitions we have in NoHeader RDD
println("No.ofpartitions="+emsNoHeader.partitions.size)

//Find number of entries with 10003 zip code
val zip10003_RDD = emsNoHeader.filter(line => line.contains("10003"))

//Print your 10003 zip code RDD
zip10003_RDD.collect().foreach(println)

// MAP THE RECORDS TO A KEY AND A VALUE
val zipCountRDD = emsNoHeader.map(line => {
  val cols = line.split(",")
  val zip = cols(21)
  (zip, 1)
})

// Reduce by key to count the number of records for each zip code
val zipCountSumRDD = zipCountRDD.reduceByKey(_ + _)

// FIND THE DISTINCT VALUES IN EACH COLUMN (COLUMNS I AM USING FOR MY OWN ANALYTIC = ZIP CODE)
zipCountSumRDD.collect().foreach(println)

// COUNT THE NUMBER OF RECORDS USING MAP() FUNCTION
val zipListRDD = emsNoHeader.map(line => 1)
val CountRecs = zipListRDD.reduce((count1, count2) => count1 + count2)
