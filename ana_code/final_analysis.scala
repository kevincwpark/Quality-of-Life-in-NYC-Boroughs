//Start Spark REPL
spark-shell --deploy-mode client

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

import org.apache.spark.ml.feature.{MinMaxScaler, MinMaxScalerModel, VectorAssembler}

val assembler = new VectorAssembler()
  .setInputCols(Array("leadAvg"))
  .setOutputCol("leadAvgFeature")
val assembledDF = assembler.transform(finalDF)

val assembler2 = new VectorAssembler()
  .setInputCols(Array("copperAvg"))
  .setOutputCol("copperAvgFeature")
val assembledDF2 = assembler2.transform(assembledDF)

val assembler3 = new VectorAssembler()
  .setInputCols(Array("propVal"))
  .setOutputCol("propValFeature")
val assembledDF3 = assembler3.transform(assembledDF2)

val assembler4 = new VectorAssembler()
  .setInputCols(Array("emsCount"))
  .setOutputCol("emsCountFeature")
val finalAssembledDF = assembler4.transform(assembledDF3)

val scaler = new MinMaxScaler()
  .setMin(-5)
  .setMax(0)
  .setInputCol("leadAvgFeature")
  .setOutputCol("leadAvgScaled")
val scalerModel = scaler.fit(finalAssembledDF)
val scaledDF = scalerModel.transform(finalAssembledDF).drop("leadAvgFeature")

val scaler2 = new MinMaxScaler()
  .setMin(-5)
  .setMax(0)
  .setInputCol("copperAvgFeature")
  .setOutputCol("copperAvgScaled")
val scalerModel2 = scaler2.fit(scaledDF)
val scaledDF2 = scalerModel2.transform(scaledDF).drop("copperAvgFeature")

val scaler3 = new MinMaxScaler()
  .setMin(0)
  .setMax(10)
  .setInputCol("propValFeature")
  .setOutputCol("propValScaled")
val scalerModel3 = scaler3.fit(scaledDF2)
val scaledDF3 = scalerModel3.transform(scaledDF2).drop("propValFeature")

val scaler4 = new MinMaxScaler()
  .setMin(-10)
  .setMax(0)
  .setInputCol("emsCountFeature")
  .setOutputCol("emsCountScaled")
val scalerModel4 = scaler4.fit(scaledDF3)
val scaledDF4 = scalerModel4.transform(scaledDF3).drop("emsCountFeature")

val finalDF = scaledDF4.drop("propVal").drop("emsCount").drop("leadAvg").drop("copperAvg")
finalDF.show(false)

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.udf
val extractFirst = udf((v: Vector) => v(0))
val numericDF1 = finalDF.withColumn("propValNumeric", extractFirst($"propValScaled")).drop("propValScaled")
val numericDF2 = numericDF1.withColumn("emsCountNumeric", extractFirst($"emsCountScaled")).drop("emsCountScaled")
val numericDF3 = numericDF2.withColumn("leadAvgNumeric", extractFirst($"leadAvgScaled")).drop("leadAvgScaled")
val numericDF = numericDF3.withColumn("copperAvgNumeric", extractFirst($"copperAvgScaled")).drop("copperAvgScaled")

import org.apache.spark.sql.functions.abs
val positiveDF = numericDF.select($"zip", $"borough", $"propValNumeric", abs($"emsCountNumeric").as("emsCountNumeric"), abs($"leadAvgNumeric").as("leadAvgNumeric"), abs($"copperAvgNumeric").as("copperAvgNumeric"))

val positiveRDD = positiveDF.rdd
positiveRDD.collect().foreach(println)
val sumMapDF = positiveRDD.map(row => {
    val zip = row.getString(0)
    val borough = row.getString(1)
    val prop = row.getDouble(2)
    val ems = row.getDouble(3)
    val lead = row.getDouble(4)
    val copper = row.getDouble(5)
    (zip, borough, prop + ems + lead + copper)
}).toDF("zip", "borough", "sum")
sumMapDF.show(false)

import org.apache.spark.sql.functions.round
val roundedDF = sumMapDF.withColumn("sum", round($"sum", 2))

import org.apache.spark.sql.functions.desc
val sortedDF = roundedDF.sort(desc("sum"))
sortedDF.count()
sortedDF.show(109)

sortedDF.write.format("csv")
  .option("header", "true")
  .save("hdfs://nyu-dataproc-m/user/ic2184_nyu_edu/final_project_18/results.csv")