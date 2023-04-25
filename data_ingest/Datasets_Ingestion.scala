val water_qualityRDD = sc.textFile("water_quality.csv")
val waterHead = water_qualityRDD.first()
val waterNoHeader = water_qualityRDD.filter(line => !line.equals(waterHead))
val waterColumnsRDD = waterNoHeader.map(line => line.split(","))
case class WaterQuality(zip: String, borough: String, leadLevel: Double, copperLevel: Double)
val waterDF = waterColumnsRDD.map(arr => WaterQuality(arr(2), arr(1).toUpperCase(), arr(5).toDouble, arr(6).toDouble)).toDF()
val waterSelectedColumnsDF = waterDF.select("zip", "borough", "leadLevel", "copperLevel")
val finalWaterDF = waterSelectedColumnsDF.withColumn("borough", when(col("borough") === "NEW YORK", "MANHATTAN").otherwise(col("borough")))
val filteredDF = finalWaterDF.filter($"zip" =!= 11358 && $"zip" =!= 11421 && $"zip" =!= 11225 && $"zip" =!= 11428 && $"zip" =!= 11433)
val finalWaterRDD = filteredDF.rdd
val waterZipMap = finalWaterRDD.map(row => {
    val zip = row.getString(0)
    val borough = row.getString(1)
    val lead = row.getDouble(2)
    val copper = row.getDouble(3)
    ((zip, borough), (lead, copper, 1))
})
val waterZipVals = waterZipMap.reduceByKey((value1, value2) => (value1._1 + value2._1, value1._2 + value2._2, value1._3 + value2._3))
val waterZipAverages = waterZipVals.mapValues{case (lead, copper, count) => (lead/count, copper/count)}
val waterZipResultRDD = waterZipAverages.map{case ((zip, borough), (leadAvg, copperAvg)) => s"$zip, $borough, $leadAvg, $copperAvg"}
val waterResultDF = waterZipResultRDD.map(row => {
    val Array(zip, borough, leadAvg, copperAvg) = row.split(",")
    (zip, borough, leadAvg.toDouble, copperAvg.toDouble)
  }).toDF("zip", "borough", "leadAvg", "copperAvg")
waterResultDF.rdd.collect().foreach(println)

val EMS_RDD = sc.textFile("ems_incident_dispatch_data.csv")
val emsHead = EMS_RDD.first()
val emsNoHeader = EMS_RDD.filter(line => !line.equals(emsHead))
val emsColumnsRDD = emsNoHeader.map(line => line.split(","))
case class EMSIncident(zip: String, borough: String)
val emsDF = emsColumnsRDD.map(arr => EMSIncident(arr(21), arr(19).toUpperCase())).toDF()
val emsSelectedColumnsDF = emsDF.select("zip", "borough")
val emsFilteredDF = emsSelectedColumnsDF.filter($"zip".rlike("^[0-9]{5}$") && $"borough".isNotNull)
val emsFilteredDF2 = emsFilteredDF.withColumn("borough", when(col("borough") === "RICHMOND / STATEN ISLAND", " STATEN ISLAND").otherwise(col("borough")))
val emsFilteredDF3 = emsFilteredDF2.withColumn("borough", when(col("borough") === "STATEN ISLAND", " STATEN ISLAND").otherwise(col("borough")))
val emsFilteredDF4 = emsFilteredDF3.withColumn("borough", when(col("borough") === "MANHATTAN", " MANHATTAN").otherwise(col("borough")))
val emsFilteredDF5 = emsFilteredDF4.withColumn("borough", when(col("borough") === "BRONX", " BRONX").otherwise(col("borough")))
val emsFilteredDF6 = emsFilteredDF5.withColumn("borough", when(col("borough") === "QUEENS", " QUEENS").otherwise(col("borough")))
val finalemsDF = emsFilteredDF6.withColumn("borough", when(col("borough") === "BROOKLYN", " BROOKLYN").otherwise(col("borough")))
val finalemsRDD = finalemsDF.rdd
val emsZipMap = finalemsRDD.map(row => {
    val zip = row.getString(0)
    val borough = row.getString(1)
    ((zip, borough), (1))
})
val emsZipVals = emsZipMap.reduceByKey((value1, value2) => (value1 + value2))
val emsResultDF = emsZipVals.map(row => {
    val ((zip, borough), emsCount) = row
    (zip, borough, emsCount)
}).toDF("zip", "borough", "emsCount")
emsResultDF.rdd.collect().foreach(println)

val propertyValRDD = sc.textFile("Revised_Notice_of_Property_Value__RNOPV_.csv")
val Head = propertyValRDD.first()
val NoHeader = propertyValRDD.filter(line => !line.equals(Head))
val ColumnsRDD = NoHeader.map(line => line.split(","))
case class PropertyVal(zip: String, borough: String, propVal: Double)
val propDF = ColumnsRDD.filter(arr => arr.length >= 32 && arr(12).matches("^\\d+$") && arr(12).toInt != 0 && arr(12).toInt != 2).map(arr => PropertyVal(arr(30), arr(29).toUpperCase(), arr(12).toDouble)).toDF()
val propDFClean = propDF.withColumn("borough", when(col("borough") === "STATEN IS", "STATEN ISLAND").otherwise(col("borough")))
val propSelectedColumnsDF = propDFClean.select("zip", "borough", "propVal")
val finalPropRDD = propSelectedColumnsDF.rdd
val propertyZipMap = finalPropRDD.map(row => {
    val zip = row.getString(0)
    val borough = row.getString(1)
    val value = row.getDouble(2)
    ((zip, borough), (value, 1))
})
val propertyZipVals = propertyZipMap.reduceByKey((value1, value2) => (value1._1 + value2._1, value1._2 + value2._2))
val propertyAvgZipVals = propertyZipVals.mapValues { case (sum, count) => sum/count }
val propertyZipResultRDD = propertyAvgZipVals.filter { case ((zip, borough), _) =>
  zip != null && !zip.isEmpty && borough != null && !borough.isEmpty
}.map { case ((zip, borough), (value)) => s"$zip, $borough, $value" }
val propResultDF = propertyZipResultRDD.map(row => {
    val Array(zip, borough, value) = row.split(",")
    (zip, borough, value.toDouble)
  }).toDF("zip", "borough", "propVal")
propResultDF.rdd.collect().foreach(println)

val joinedDF = propResultDF.join(waterResultDF, Seq("zip", "borough"), "inner")
val finalDF = joinedDF.join(emsResultDF, Seq("zip", "borough"), "inner")
finalDF.show()