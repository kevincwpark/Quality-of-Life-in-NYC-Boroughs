//Read File
val propertyValRDD = sc.textFile("Revised_Notice_of_Property_Value__RNOPV_.csv")
val Head = propertyValRDD.first()
val NoHeader = propertyValRDD.filter(line => !line.equals(Head))
//No header 
val ColumnsRDD = NoHeader.map(line => line.split(","))
//Create a base class
case class PropertyVal(ZipCode: String, Borough: String, PropertyValue: Int)
//Cleaning removing rows that include non-property values,
//separation rows (#2), and rows with property value of 0
//Also did Text Formatting making Boroughs all Capital 
val propDF = ColumnsRDD.filter(arr => arr.length >= 32 && arr(12).matches("^\\d+$") && arr(12).toInt != 0 && arr(12).toInt != 2).map(arr => PropertyVal(arr(30), arr(29).toUpperCase(), arr(12).toInt)).toDF()
val propSelectedColumnsDF = propDF.select("ZipCode", "Borough", "PropertyValue")



//Get the average property value for each zip code through a map reduce
val avgPropValDF = propSelectedColumnsDF
    .distinct() // Add distinct operation
    .groupBy("ZipCode")
    .agg(avg("PropertyValue").alias("AvgPropertyValue"))

// Print the results
avgPropValDF.show()

//Doing the same, just easier get the average of each borough
val boroughAvgDF = propDF.groupBy("Borough").avg("PropertyValue")
boroughAvgDF.show()


//Find the mean
val meanPropValue = propSelectedColumnsDF.agg(avg("PropertyValue")).first().getDouble(0)
println(s"Mean Property Value: $meanPropValue")
//Find mode
val modeDF = propSelectedColumnsDF.groupBy("PropertyValue").agg(count("*").alias("count")).sort(desc("count")).limit(1)
val mode = modeDF.first().getAs[Int]("PropertyValue")
//Find the median
val median = propSelectedColumnsDF.select("PropertyValue").sort("PropertyValue").agg(expr("percentile_approx(PropertyValue, 0.5)").alias("median")).first()


