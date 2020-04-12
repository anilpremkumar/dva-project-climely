// Databricks notebook source
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, IntegerType}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._

// COMMAND ----------

/////////////////////////////////////////////////////////////////////////////////
///////////////Code to extract and transform PARTICULATES data///////////////////
/////////////////////////////////////////////////////////////////////////////////

// COMMAND ----------

// Arun's reusable script to load input files into dataframe and calculate aggregate
var alldf = spark.emptyDataFrame;
for( a <- 1982 to 2019){
	println( "Value of a: " + a );
	var df = spark.read.format("csv").option("header", "true").load("/FileStore/tables/daily_81102_"+a+".csv");
	df = df.withColumn("Date_Local", (col("Date Local").cast("timestamp")))
	df = df.withColumn("Arithmetic_Mean", df("Arithmetic Mean").cast("float"));
	df = df.withColumn("County_Name", (col("County Name").cast("string")))
	df = df.withColumn("State_Name", (col("State Name").cast("string")))
    df = df.withColumn("Parameter_Name", (col("Parameter Name").cast("string")))
    df = df.withColumn("Units_of_Measure", (col("Units of Measure").cast("string")))
	if (alldf.count == 0 ){
		alldf = df
	}else{
		alldf = alldf.union(df)
	}
}
alldf.createOrReplaceTempView("particulates_pm10_mass");

var comb1 = spark.sql("""SELECT County_Name,State_Name,date_format(Date_Local,"yyyy-MM-01") AS year_month, avg(Arithmetic_Mean) AS particulates_pm10_mass FROM particulates_pm10_mass GROUP BY County_Name,State_Name,date_format(Date_Local,"yyyy-MM-01")""")
comb1 = comb1.withColumn("year_month", (col("year_month").cast("timestamp")))
comb1.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/FileStore/particulates_pm10_agg.csv")

// COMMAND ----------

// Arun's reusable script to load input files into dataframe and calculate aggregate
var alldf = spark.emptyDataFrame;
for( a <- 1980 to 2019){
	println( "Value of a: " + a );
	var df = spark.read.format("csv").option("header", "true").load("/FileStore/tables/daily_LEAD_"+a+".csv");
	df = df.withColumn("Date_Local", (col("Date Local").cast("timestamp")))
	df = df.withColumn("Arithmetic_Mean", df("Arithmetic Mean").cast("float"));
	df = df.withColumn("County_Name", (col("County Name").cast("string")))
	df = df.withColumn("State_Name", (col("State Name").cast("string")))
    df = df.withColumn("Parameter_Name", (col("Parameter Name").cast("string")))
    df = df.withColumn("Units_of_Measure", (col("Units of Measure").cast("string")))
	if (alldf.count == 0 ){
		alldf = df
	}else{
		alldf = alldf.union(df)
	}
}
alldf.createOrReplaceTempView("toxics_lead");

var comb1 = spark.sql("""SELECT County_Name,State_Name,date_format(Date_Local,"yyyy-MM-01") AS year_month, avg(Arithmetic_Mean) AS lead_avg FROM toxics_lead GROUP BY County_Name,State_Name,date_format(Date_Local,"yyyy-MM-01")""")
comb1 = comb1.withColumn("year_month", (col("year_month").cast("timestamp")))
comb1.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/FileStore/toxics_lead_agg.csv")

// COMMAND ----------

// Script I created to read and load data. But we decided to use Arun's script mentioned above
// val file_names_raw = """
//  File uploaded to /FileStore/tables/daily_81102_1982.csv
//  File uploaded to /FileStore/tables/daily_81102_1983.csv
//  File uploaded to /FileStore/tables/daily_81102_1984.csv
//  File uploaded to /FileStore/tables/daily_81102_1985.csv
//  File uploaded to /FileStore/tables/daily_81102_1986.csv
//  File uploaded to /FileStore/tables/daily_81102_1988.csv
//  File uploaded to /FileStore/tables/daily_81102_1987.csv
//  File uploaded to /FileStore/tables/daily_81102_1989.csv
//  File uploaded to /FileStore/tables/daily_81102_1990.csv
//  File uploaded to /FileStore/tables/daily_81102_1991.csv
//  File uploaded to /FileStore/tables/daily_81102_1992.csv
//  File uploaded to /FileStore/tables/daily_81102_1993.csv
//  File uploaded to /FileStore/tables/daily_81102_1994.csv
//  File uploaded to /FileStore/tables/daily_81102_1995.csv
//  File uploaded to /FileStore/tables/daily_81102_1996.csv
//  File uploaded to /FileStore/tables/daily_81102_1997.csv
//  File uploaded to /FileStore/tables/daily_81102_1998.csv
//  File uploaded to /FileStore/tables/daily_81102_1999.csv
//  File uploaded to /FileStore/tables/daily_81102_2000.csv
//  File uploaded to /FileStore/tables/daily_81102_2001.csv
//  File uploaded to /FileStore/tables/daily_81102_2002.csv
//  File uploaded to /FileStore/tables/daily_81102_2003.csv
//  File uploaded to /FileStore/tables/daily_81102_2004.csv
//  File uploaded to /FileStore/tables/daily_81102_2005.csv
//  File uploaded to /FileStore/tables/daily_81102_2006.csv
//  File uploaded to /FileStore/tables/daily_81102_2007.csv
//  File uploaded to /FileStore/tables/daily_81102_2008.csv
//  File uploaded to /FileStore/tables/daily_81102_2009.csv
//  File uploaded to /FileStore/tables/daily_81102_2010.csv
//  File uploaded to /FileStore/tables/daily_81102_2011.csv
//  File uploaded to /FileStore/tables/daily_81102_2012.csv
//  File uploaded to /FileStore/tables/daily_81102_2013.csv
//  File uploaded to /FileStore/tables/daily_81102_2014.csv
//  File uploaded to /FileStore/tables/daily_81102_2015.csv
//  File uploaded to /FileStore/tables/daily_81102_2016.csv
//  File uploaded to /FileStore/tables/daily_81102_2017.csv
//  File uploaded to /FileStore/tables/daily_81102_2018.csv
//  File uploaded to /FileStore/tables/daily_81102_2019.csv
// """;
// val file_names_array = pm10_databricks_file_names_raw.split("\n")
// val schema_of_df = new StructType()
//   .add("Date Local",StringType,true)
//   .add("Parameter Name",IntegerType,true)
//   .add("Units of Measure",StringType,true)
//   .add("Arithmetic Mean",StringType,true)
//   .add("State Name",StringType,true)
//   .add("County Name",StringType,true)
// display(schema_of_df)

// Iterate through array of file names, create dataframes and combine them into one
// for(fn <- file_names_array) {
//   // check if element is null. Proceed only if it is not
//   if (fn != "") {
//     val file_name = fn.replace(" File uploaded to ", "")
//     if (file_name == "/FileStore/tables/daily_81102_1982.csv") {
//       // Loading csv input files containing Storm Events data into Spark dataframes
//       val df_raw_data = spark.read
//          .format("com.databricks.spark.csv")
//          .option("header", "true") // Use first line of all files as header
//          .option("nullValue", "null")
//          .load(file_name)
//       // Selecting only necessary columns and creating temp tables
//       df_raw_data.select($"Date Local", $"Parameter Name", $"Units of Measure", $"Arithmetic Mean", $"State Name", $"County Name"
//                         ).createOrReplaceTempView("temp_table_raw_data")
//     }
//   }
// }
