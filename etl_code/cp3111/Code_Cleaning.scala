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

// Write to HDFS
emsSelectedColumnsDF.write
  .option("header", "true")
  .option("sep", ",")
  .csv("hdfs://nyu-dataproc-m/user/cp3111_nyu_edu/ems_clean2.csv")
