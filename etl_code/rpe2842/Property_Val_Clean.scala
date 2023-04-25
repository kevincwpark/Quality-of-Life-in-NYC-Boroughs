
val propertyValRDD = sc.textFile("Revised_Notice_of_Property_Value__RNOPV_.csv")
val Head = propertyValRDD.first()
val NoHeader = propertyValRDD.filter(line => !line.equals(Head))
val ColumnsRDD = NoHeader.map(line => line.split(","))
case class PropertyVal(zip: String, borough: String, propVal: Double)
val propDF = ColumnsRDD.filter(arr => arr.length >= 32 && arr(12).matches("^\\d+$") && arr(12).toInt != 0 && arr(12).toInt != 2).map(arr => PropertyVal(arr(30), arr(29).toUpperCase(), arr(12).toDouble)).toDF()
val propDFClean = propDF.withColumn("borough", when(col("borough") === "STATEN IS", "STATEN ISLAND").otherwise(col("borough")))
val propSelectedColumnsDF = propDFClean.select("zip", "borough", "propVal")
val finalPropRDD = propSelectedColumnsDF.rdd
finalPropRDD.collect.foreach(println)