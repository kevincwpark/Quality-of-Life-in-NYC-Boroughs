// Start Spark REPL
spark-shell --deploy-mode client

// Load the CSV file into an RDD
val EMS_RDD = sc.textFile("ems_incident_dispatch_data.csv")

// Remove the header row
val emsHead = EMS_RDD.first()
val emsNoHeaderRDD = EMS_RDD.filter(line => !line.equals(emsHead))

// Split each row into an array of columns
val emsColumnsRDD = emsNoHeaderRDD.map(line => line.split(","))

// Define the schema of the DataFrame using a case class
case class EMSIncident(callType: String, zipCode: String, borough: String)

// Convert the RDD to a DataFrame
val emsDF = emsColumnsRDD.map(arr => EMSIncident(arr(2), arr(21), arr(19))).toDF()

// HW9 UPDATE: Filter out rows with invalid zipcodes, null zipcodes, or null boroughs
val emsFilteredDF = emsDF.filter($"zipCode".rlike("^[0-9]{5}$") && $"borough".isNotNull)

// Convert boroughs to all capital letters
val emsCapitalDF = emsFilteredDF.withColumn("borough", upper($"borough"))

// Select only the columns you need
val emsSelectedColumnsDF = emsCapitalDF.select("zipCode", "borough", "callType")

// Show the first 20 rows of the DataFrame
emsSelectedColumnsDF.show(20)

// Load the CSV file into a DataFrame
val emsDF = emsSelectedColumnsDF

// Find the distinct values in each column
val distinctZipCodes = emsDF.select("zipCode").distinct().count()
val distinctBoroughs = emsDF.select("borough").distinct().count()
val distinctCallTypes = emsDF.select("callType").distinct().count()

// Calculate the mean, median, and zipcode with the most amount of EMS incidents
val incidentCounts = emsDF.groupBy("zipCode").count()
val meanRecords = incidentCounts.agg(avg("count")).collect()(0)(0).asInstanceOf[Double]
val medianRecords = incidentCounts.stat.approxQuantile("count", Array(0.5), 0.25)(0)
val maxZipcode = incidentCounts.orderBy(desc("count")).first().getString(0)

// Print the results
println(s"Distinct Zip Codes: $distinctZipCodes")
println(s"Distinct Boroughs: $distinctBoroughs")
println(s"Distinct Call Types: $distinctCallTypes")
println(s"Mean Records of EMS Incidents per Zip Code: $meanRecords")
println(s"Median Records of EMS Incidents per Zip Code: $medianRecords")
println(s"Zipcode with the most EMS Incidents: $maxZipcode")
