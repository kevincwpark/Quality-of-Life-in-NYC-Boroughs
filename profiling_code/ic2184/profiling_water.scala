//SCALA SCRIPT TO MAKE RDD OF WATER QUALITY
val water_qualityRDD1 = sc.textFile("water_quality.csv")

//REMOVE HEADERS
val Head = water_qualityRDD1.first()
val NoHeader = water_qualityRDD1.filter(line => !line.equals(Head))

//SPLIT ROWS INTO ARRAY OF COLUMNS
val ColumnsRDD = NoHeader.map(line => line.split(","))

//DEFINE SCHEMA OF DATAFRAME W/ CASE CLASS
case class WaterQuality(Borough: String, ZipCode: String, LeadLevel: Double, CopperLevel: Double)

//CONVERT RDD INTO DATAFRAME 
//CLEANING BY CONVERTING BOROUGH TO UPPERCASE AND LEAD/COPPER LEVELS TO DOUBLES
val waterDF = ColumnsRDD.map(arr => WaterQuality(arr(1).toUpperCase(), arr(2), arr(5).toDouble, arr(6).toDouble)).toDF()

//SELECT ONLY COLUMNS 1, 2, 5, AND 6 (NO NULL VALUES AND NOTHING ADDED TO DATA)
val waterSelectedColumnsDF = waterDF.select("Borough", "ZipCode", "LeadLevel", "CopperLevel")

//ADDITIONAL CLEANING
//CONVERT NEW YORK BOROUGH NAME INTO MANHATTAN
val finalWaterDF = waterSelectedColumnsDF.withColumn("Borough", when(col("Borough") === "NEW YORK", "MANHATTAN").otherwise(col("Borough")))

//REMOVE ZIPCODES CORRESPONDING TO OUTLIERS
val filteredDF = finalWaterDF.filter($"zip" =!= 11358 && $"zip" =!= 11421 && $"zip" =!= 11225 && $"zip" =!= 11428 && $"zip" =!= 11433)

//DISPLAY NEW DATAFRAME
filteredDF.show()



//EXPLORE DATA
//SEE NUMBER OF PARTITIONS - RETURNS 2
println("No.ofpartitions="+NoHeader.partitions.size)

//FIND ENTRIES WITH ZIPCODE 11209
val checkzipRDD = NoHeader.filter(line => line.contains("11209"))

//PRINT RDD FOR 11209 ZIPS
checkzipRDD.collect().foreach(println)

//COUNT NUMBER OF RECORDS WITH COUNT() FUNCTION
NoHeader.count()

//MAPPER TO COUNT NUMBER OF RECORDS ALONE
val zipListRDD = NoHeader.map(line => 1)

//REDUCER TO COUNT NUMBER OF RECORDS
val CountRecs = zipListRDD.reduce((count1, count2) => count1 + count2)

//MAP RECORDS TO KEY-VALUE PAIRS WITH KEY = ZIP, VALUE = (LEAD LEVEL, COPPER LEVEL)
val zipLeadCopperRDD = NoHeader.map(line => {
  val cols = line.split(",")
  val zip = cols(2)
  val lead = cols(5).toDouble
  val copper = cols(6).toDouble
  (zip, (lead, copper))
})

//REDUCE BY KEY TO GET SUMMED VALUES FOR EACH KEY
val zipLeadCopperSumRDD = zipLeadCopperRDD.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

//DISPLAY SUMMED KEY-VALUE PAIR
zipLeadCopperSumRDD.toDF().show()

//COUNTING DISTINCT BOROUGHS 
val countDistinctBorough = NoHeader.map(line => {
    val cols = line.split(",")
    val borough = cols(1)
    (borough, 1)
})
val countDistinctBoroughSum = countDistinctBorough.reduceByKey(_ + _)
countDistinctBoroughSum.collect().foreach(println)

//COUNTING DISTINCT ZIPCODES 
val countDistinctZip = NoHeader.map(line => {
    val cols = line.split(",")
    val zip = cols(2)
    (zip, 1)
})
val countDistinctZipSum = countDistinctZip.reduceByKey(_ + _)
countDistinctZipSum.collect().foreach(println)


//COUNTING DISTINCT LEAD LEVELS
val countDistinctLead = NoHeader.map(line => {
    val cols = line.split(",")
    val lead = cols(5)
    (lead, 1)
})
val countDistinctLeadSum = countDistinctLead.reduceByKey(_ + _)
countDistinctLeadSum.collect().foreach(println)


//COUNTING DISTINCT COPPER LEVELS
val countDistinctCopper = NoHeader.map(line => {
    val cols = line.split(",")
    val copper = cols(6)
    (copper, 1)
})
val countDistinctCopperSum = countDistinctCopper.reduceByKey(_ + _)
countDistinctCopperSum.collect().foreach(println)


//GETTING AVERAGE VALUE OF LEAD DATA
//MAPPING LEAD VALUES TO KEYS AND A TALLY 1 TO VALUES
val leadRDD = NoHeader.map(line => {
  val cols = line.split(",")
  val lead = cols(5).toDouble
  (lead, 1)
})

//SUMMING ALL LEAD LEVELS AND TOTAL COUNT AND PUTTING INTO RDD
val leadsumRDD = leadRDD.reduce((x, y) => (x._1 + y._1, x._2 + y._2))

//AVERAGE LEAD VALUE IS EQUAL TO INDEX 1 (SUM OF COPPER VALS) DIVIDED BY INDEX 2 (COUNT)
val avgLead: Double = leadsumRDD._1 / leadsumRDD._2
println(avgLead)

//GETTING AVERAGE VALUE OF LEAD DATA
//MAPPING COPPER VALUES TO KEYS AND A TALLY 1 TO VALUES
val copperRDD = NoHeader.map(line => {
  val cols = line.split(",")
  val copper = cols(6).toDouble
  (copper, 1)
})

//SUMMING ALL COPPER LEVELS AND TOTAL COUNT AND PUTTING INTO RDD
val coppersumRDD = copperRDD.reduce((x, y) => (x._1 + y._1, x._2 + y._2))

//AVERAGE COPPER VALUE IS EQUAL TO INDEX 1 (SUM OF COPPER VALS) DIVIDED BY INDEX 2 (COUNT)
val avgCopper: Double = coppersumRDD._1 / coppersumRDD._2
println(avgCopper)


//GET MEDIAN OF LEAD DATA WITH FUNCTION
val leadMedian = filteredDF.stat.approxQuantile("LeadLevel", Array(0.5), 0.0)(0)

//GET MEDIAN OF COPPER DATA WITH FUNCTION
val copperMedian = filteredDF.stat.approxQuantile("CopperLevel", Array(0.5), 0.0)(0)


//GET MODE OF LEAD DATA
//CREATE DATAFRAME OF DISTINCT LEAD VALUE COUNTS
val leadDF = countDistinctLeadSum.toDF()

//SORT DATAFRAME BY IN DESCENDING ORDER OF FREQUENCY
val leadmodeDF = leadDF.sort(desc("_2"))

//SELECT THE FIRST ITEM IN THE SORTED DATAFRAME TO GET THE MOST FREQUENT LEAD VALUE
val leadmode = leadmodeDF.select("_1").first().getString(0)
println(leadmode)

//GET MODE OF COPPER DATA
//CREATE DATAFRAME OF DISTINCT COPPER VALUE COUNTS
val copperDF = countDistinctCopperSum.toDF()

//SORT DATAFRAME BY IN DESCENDING ORDER OF FREQUENCY
val coppermodeDF = copperDF.sort(desc("_2"))

//SELECT THE FIRST ITEM IN THE SORTED DATAFRAME TO GET THE MOST FREQUENT COPPER VALUE
val coppermode = coppermodeDF.select("_1").first().getString(0)
println(coppermode)