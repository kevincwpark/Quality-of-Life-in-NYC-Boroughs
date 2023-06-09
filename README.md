# Quality-of-Life-in-NYC-Boroughs
This program will assemble data regarding water quality, frequency of EMS dispatches, and property valuation in each of NYC’s boroughs to determine which has the best quality of life and inform residents of the best areas to live in. Poor water quality can lead to various health issues among residents. A higher property valuation suggests that the community is desirable to live in, which can be attributed to factors such as a low crime rate, good schools, and well-maintained infrastructure. Finally, high frequency of EMS dispatch could suggest a higher prevalence of medical emergencies, accidents, or other dangerous incidents. This project aims to quantify and compare these levels, stratifying based on the zipcodes which each dataset contains.

## Directory Structure

The directory structure of the project is as follows:

- `/ana_code`: contains the final and completed build of the source code, all pieced together and ready to run
- `/data_ingest`: contains code and commands for data ingestion
- `/etl_code`: contains code from the early invidiual ETL/cleaning step, subdirectories for each team member
- `/profiling_code`: contains code from the early individual profiling step, subdirectories for each team member
- `/test_code`: directory containing test code and unused code
- `/screenshots`: contains screenshots that show the final analysis running through each step
- `README.md`: readme file describing the project and how to build and run the code

## File Description
### Non-specific files
- `/ana_code/final_analysis.scala`: Reads data from the three csv files (`water_quality.csv`, `ems_incident_dispatch_data.csv`, `Revised_Notice_of_Property_Value__RNOPV_.csv`), cleans and processes the data, and then joins them based on zip code and borough. It calculates the average lead and copper level for each zip code and borough from water quality data, and the count of emergency incidents in each zip code and borough from EMS data. Finally, it extracts the property value and borough information from property value data and joins all three datasets based on zip code and borough, and prints the resulting dataframe.
- `/data_ingest/Datasets_Ingestion.scala`: Reads data from the three csv files (`water_quality.csv`, `ems_incident_dispatch_data.csv`, `Revised_Notice_of_Property_Value__RNOPV_.csv`) to provide useful information about them and process them for the normalization steps in the `final_analysis.scala`.

### Kevin (cp3111) specific files
- `/etl_code/cp3111/Code_Cleaning (Initial).scala`: Initial dataset cleaning for the EMS dispatch dataset: removes the header row, splits each row into an array of columns, converts the RDD to a DataFrame, filters out invalid and null zip codes and null boroughs, converts boroughs to all capital letters, and selects only the needed columns.
- `/etl_code/cp3111/Code_Cleaning (Final).scala`: Shares the same functions as the initial dataset cleaning, with added formatting changes to be compatible with the other datasets.
- `/profiling_code/cp3111/Code_Analysis.scala`: Takes the dataframe from the initial cleaning steps to find the distinct values in each column, calculate the mean, median, and zipcode with the most amount of EMS incidents, and print the results.
- `/test_code/EMSCountRecs.scala`: Count the # of records in total, and for a specific zip code using mapreduce. Used to explore the data using scala in the earlier steps of the project, and is not used in the final analysis.

### Irvin (ic2184) specific files
- `/etl_code/ic2184/cleaning_water.scala`: Initial dataset cleaning for the water quality dataset: removes the header row, splits each row into an array of columns, converts the RDD to a DataFrame, converts boroughs to all capital letters, relabels all boroughs falsely named "NEW YORK" into one named "MANHATTAN", filters out rows corresponding to outliers in lead and copper data, and selects only the columns needed for the analytic.
- `/profiling_code/ic2184/profiling_water.scala`: Takes the dataframe from the initial cleaning steps to count the number of partitions, finds and counts the number of entries corresponding to some arbitrary zipcode chosen to be "11209", finds and prints the distinct values in each column, and calculates the mean, median, and most frequently occurring lead and copper values.

### Ricardo (rpe2842) specific files
- `/etl_code/rpe2842/Property_Val_Clean.scala`: Initial dataset cleaning for the Property Value which removes the header row. Then it creates a Spark DataFrame with a subset of the columns from the file (specifically, columns 29, 30, and 12) by filtering for rows with the proper format and creating a case class for the data. The DataFrame is then cleaned by standardizing the borough names, selecting only certain columns, and converting to an RDD for output. Finally, it prints out the contents of the RDD using println. 
- `/profiling_code/rpe2842/Property_Val_Profiling.scala`: It reads the property value csv file and removes the header from it. Then it creates a Spark DataFrame with a subset of the columns from the file (specifically, columns 29, 30, and 12) by filtering for rows with the proper format and creating a case class for the data. The DataFrame is then briefly cleaned by standardizing the borough names, selecting only certain columns, and aggregating the average property value by zip code and borough. Additionally, the code calculates the mean, mode, and median property values from the filtered DataFrame. Finally, it prints out the results of the calculations.


## Data
The input data used in this project can be found at `final_project_18` under the files named `water_quality.csv`, `ems_incident_dispatch_data.csv`, and `Revised_Notice_of_Property_Value__RNOPV_.csv`. Each respective file was retrieved from NYC Open Data.  

- [EMS Incident Dispatch Data](https://data.cityofnewyork.us/Public-Safety/EMS-Incident-Dispatch-Data/76xm-jjuj)
- [Compliance at-the-tap Lead and Copper Data](https://data.cityofnewyork.us/Environment/Compliance-at-the-tap-Lead-and-Copper-Data/3wxk-qa8q)
- [Revised Notice of Property Value (RNOPV)](https://data.cityofnewyork.us/City-Government/Revised-Notice-of-Property-Value-RNOPV-/8vgb-zm6e)


## Build and Run Instructions
1. Clone the repository to your local machine.
2. Navigate to the hdfs directory named `hdfs://nyu-dataproc-m/user/ic2184_nyu_edu/final_project_18` to access the 3 CSV input data files mentioned above (if access is shared). Otherwise, download and upload aforementioned CSV files from NYC Open Data into HDFS - may need to rename CSV files to run programs directly.
3. Navigate to the `/ana_code` directory and run the source code for the analytics.
4. Navigate to the `/screenshots` directory to view screenshots of the analytics running at each step.
5. Results of the analytics can be found in the output of the source code - in HDFS directory named `hdfs://nyu-dataproc-m/user/ic2184_nyu_edu/final_project_18` under the file named `results.csv`. This directory has been shared with the requested indivduals in class: cr3152, cl6405, and adm209.

Note: Specific instructions for running the code and commands may vary depending on the specific Big Data tools and cluster being used.
