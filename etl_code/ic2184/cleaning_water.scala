//SCALA SCRIPT TO MAKE RDD OF WATER QUALITY
val water_qualityRDD = sc.textFile("water_quality.csv")

//REMOVE HEADER
val waterHead = water_qualityRDD.first()
val waterNoHeader = water_qualityRDD.filter(line => !line.equals(waterHead))

//SPLIT ROWS INTO ARRAY OF COLUMNS
val waterColumnsRDD = waterNoHeader.map(line => line.split(","))

//DEFINE SCHEMA OF DATAFRAME W/ CASE CLASS
case class WaterQuality(zip: String, borough: String, leadLevel: Double, copperLevel: Double)

//CONVERT RDD INTO DATAFRAME - CAPITALIZING ALL BOROUGH NAMES FOR JOINING LATER
val waterDF = waterColumnsRDD.map(arr => WaterQuality(arr(2), arr(1).toUpperCase(), arr(5).toDouble, arr(6).toDouble)).toDF()

//SELECT ONLY COLUMNS 2, 1, 5, and 6 (NO NULL VALUES AND NOTHING ADDED TO DATA)
val waterSelectedColumnsDF = waterDF.select("zip", "borough", "leadLevel", "copperLevel")

//REPLACE ALL BOROUGHS NAMED "NEW YORK" INTO "MANHATTAN"
val finalWaterDF = waterSelectedColumnsDF.withColumn("borough", when(col("borough") === "NEW YORK", "MANHATTAN").otherwise(col("borough")))

//REMOVE ZIPCODES CORRESPONDING TO OUTLIERS
val filteredDF = finalWaterDF.filter($"zip" =!= 11358 && $"zip" =!= 11421 && $"zip" =!= 11225 && $"zip" =!= 11428 && $"zip" =!= 11433)

//DISPLAY NEW DATAFRAME
filteredDF.show()