// Start Spark REPL
spark-shell --deploy-mode client

// Load the CSV file into a DataFrame
val emsDF = spark.read.option("header", "true").csv("hdfs://nyu-dataproc-m/user/cp3111_nyu_edu/ems_clean2.csv")

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
