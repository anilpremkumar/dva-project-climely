// Databricks notebook source
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, IntegerType}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._

// COMMAND ----------

/////////////////////////////////////////////////////////////////////////////////
/////////////////Code to extract and transform STORMS data///////////////////////
/////////////////////////////////////////////////////////////////////////////////

// COMMAND ----------

// Arun's reusable script to load input files into dataframe and calculate aggregate
var alldf = spark.emptyDataFrame;
for( a <- 1980 to 2018){
	println( "Value of a: " + a );
	var df = spark.read.format("csv").option("header", "true").load("/FileStore/tables/StormEvents_details_0"+a+".csv");
	df = df.withColumn("Begin_Date_Time", (col("BEGIN_DATE_TIME").cast("string")))
	df = df.withColumn("Event_Id", df("EVENT_ID").cast("string"));
	df = df.withColumn("State", (col("STATE").cast("string")))
	df = df.withColumn("Event_Type", (col("EVENT_TYPE").cast("string")))
    df = df.withColumn("CZ_Type", (col("CZ_TYPE").cast("string")))
    df = df.withColumn("CZ_Name", (col("CZ_NAME").cast("string")))
    df = df.withColumn("Injuries_Direct", (col("INJURIES_DIRECT").cast("int")))
    df = df.withColumn("Injuries_Indirect", (col("INJURIES_INDIRECT").cast("int")))
    df = df.withColumn("Deaths_Direct", (col("DEATHS_DIRECT").cast("int")))
    df = df.withColumn("Deaths_Indirect", (col("DEATHS_INDIRECT").cast("int")))
    df = df.withColumn("Damage_Property", (col("DAMAGE_PROPERTY").cast("string")))
    df = df.withColumn("Damage_Crops", (col("DAMAGE_CROPS").cast("string")))
	if (alldf.count == 0 ){
		alldf = df
	}else{
		alldf = alldf.union(df)
	}
}
alldf.createOrReplaceTempView("storm_events_details");



// COMMAND ----------

var comb1 = spark.sql("""
SELECT CZ_Name, State, year_month, Event_Type_Group, COUNT(Event_Id) AS Count_Storm_Events,
  SUM(Injuries_Direct) + SUM(Injuries_Indirect) AS Total_Injuries,
  SUM(Deaths_Direct) + SUM(Deaths_Indirect) AS Total_Deaths,
  SUM(Damage_Property_Tranformed) + SUM(Damage_Crops_Tranformed) AS Total_Damage
FROM (
	SELECT 
		Event_Id, CZ_Name, State, date_format(to_date(Begin_Date_Time, 'dd-MMM-yy HH:mm:ss'), "yyyy-MM-01") AS year_month,
	CASE
		WHEN Event_Type = 'Coastal Flood' THEN 'Water'
		WHEN Event_Type = 'Cold/Wind Chill' THEN 'Air'
		WHEN Event_Type = 'Debris Flow' THEN 'Earth'
		WHEN Event_Type = 'Dense Fog' THEN 'Air'
		WHEN Event_Type = 'Drought' THEN 'Water'
		WHEN Event_Type = 'Dust Devil' THEN 'Air'
		WHEN Event_Type = 'Dust Storm' THEN 'Air'
		WHEN Event_Type = 'Excessive Heat' THEN 'Fire'
		WHEN Event_Type = 'Flash Flood' THEN 'Water'
		WHEN Event_Type = 'Flood' THEN 'Water'
		WHEN Event_Type = 'Frost/Freeze' THEN 'Water'
		WHEN Event_Type = 'Funnel Cloud' THEN 'Air'
		WHEN Event_Type = 'Hail' THEN 'Water'
		WHEN Event_Type = 'HAIL FLOODING' THEN 'Water'
		WHEN Event_Type = 'HAIL/ICY ROADS' THEN 'Water'
		WHEN Event_Type = 'Heat' THEN 'Fire'
		WHEN Event_Type = 'Heavy Rain' THEN 'Water'
		WHEN Event_Type = 'Heavy Snow' THEN 'Water'
		WHEN Event_Type = 'High Surf' THEN 'Water'
		WHEN Event_Type = 'High Wind' THEN 'Air'
		WHEN Event_Type = 'Hurricane (Typhoon)' THEN 'Air'
		WHEN Event_Type = 'Lightning' THEN 'Air'
		WHEN Event_Type = 'Marine High Wind' THEN 'Air'
		WHEN Event_Type = 'Rip Current' THEN 'Water'
		WHEN Event_Type = 'Seiche' THEN 'Water'
		WHEN Event_Type = 'Storm Surge/Tide' THEN 'Water'
		WHEN Event_Type = 'Strong Wind' THEN 'Air'
		WHEN Event_Type = 'Thunderstorm Wind' THEN 'Air'
		WHEN Event_Type = 'THUNDERSTORM WIND/ TREE' THEN 'Earth'
		WHEN Event_Type = 'THUNDERSTORM WIND/ TREES' THEN 'Earth'
		WHEN Event_Type = 'THUNDERSTORM WINDS FUNNEL CLOU' THEN 'Air'
		WHEN Event_Type = 'THUNDERSTORM WINDS HEAVY RAIN' THEN 'Water'
		WHEN Event_Type = 'THUNDERSTORM WINDS LIGHTNING' THEN 'Water'
		WHEN Event_Type = 'THUNDERSTORM WINDS/ FLOOD' THEN 'Water'
		WHEN Event_Type = 'THUNDERSTORM WINDS/FLASH FLOOD' THEN 'Water'
		WHEN Event_Type = 'THUNDERSTORM WINDS/FLOODING' THEN 'Water'
		WHEN Event_Type = 'THUNDERSTORM WINDS/HEAVY RAIN' THEN 'Water'
		WHEN Event_Type = 'Tornado' THEN 'Air'
		WHEN Event_Type = 'TORNADO/WATERSPOUT' THEN 'Air'
		WHEN Event_Type = 'TORNADOES, TSTM WIND, HAIL' THEN 'Air'
		WHEN Event_Type = 'Tropical Storm' THEN 'Water'
		WHEN Event_Type = 'Volcanic Ash' THEN 'Fire'
		WHEN Event_Type = 'Waterspout' THEN 'Water'
		WHEN Event_Type = 'Wildfire' THEN 'Fire'
		WHEN Event_Type = 'Winter Weather' THEN 'Water'
		ELSE 'None'
    END AS Event_Type_Group,
    Injuries_Direct,
    Injuries_Indirect,
    Deaths_Direct,
    Deaths_Indirect,
    CASE
        WHEN Damage_Property LIKE '%K' THEN cast(replace(Damage_Property, 'K', '') AS float) * 1000.0
        WHEN Damage_Property LIKE '%M' THEN cast(replace(Damage_Property, 'M', '') AS float) * 1000000.0
        ELSE Damage_Property
    END AS Damage_Property_Tranformed,
    CASE
        WHEN Damage_Crops LIKE '%K' THEN cast(replace(Damage_Crops, 'K', '') AS float) * 1000.0
        WHEN Damage_Crops LIKE '%M' THEN cast(replace(Damage_Crops, 'M', '') AS float) * 1000000.0
        ELSE Damage_Crops
    END AS Damage_Crops_Tranformed
	FROM storm_events_details
	WHERE 
		CZ_Type = 'C'
)
GROUP BY CZ_Name, State, year_month, Event_Type_Group
""")

// COMMAND ----------

comb1 = comb1.withColumn("year_month", (col("year_month").cast("timestamp")))
comb1.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/FileStore/storm_events_details_agg_new.csv")

// COMMAND ----------

// // Loading csv input files containing Storm Events data into Spark dataframes
// val df_2011_storm_events = spark.read
//    .format("com.databricks.spark.csv")
//    .option("header", "true") // Use first line of all files as header
//    .option("nullValue", "null")
//    .load("/FileStore/tables/StormEvents_details_ftp_v1_0_d2011_c20180718-5c3a7.csv")
// val df_2012_storm_events = spark.read
//    .format("com.databricks.spark.csv")
//    .option("header", "true") // Use first line of all files as header
//    .option("nullValue", "null")
//    .load("/FileStore/tables/StormEvents_details_ftp_v1_0_d2012_c20200317-ee044.csv")
// val df_2013_storm_events = spark.read
//    .format("com.databricks.spark.csv")
//    .option("header", "true") // Use first line of all files as header
//    .option("nullValue", "null")
//    .load("/FileStore/tables/StormEvents_details_ftp_v1_0_d2013_c20170519-cb7b9.csv")
// val df_2014_storm_events = spark.read
//    .format("com.databricks.spark.csv")
//    .option("header", "true") // Use first line of all files as header
//    .option("nullValue", "null")
//    .load("/FileStore/tables/StormEvents_details_ftp_v1_0_d2014_c20191116-aabf6.csv")
// val df_2015_storm_events = spark.read
//    .format("com.databricks.spark.csv")
//    .option("header", "true") // Use first line of all files as header
//    .option("nullValue", "null")
//    .load("/FileStore/tables/StormEvents_details_ftp_v1_0_d2015_c20191116-fa857.csv")
// val df_2016_storm_events = spark.read
//    .format("com.databricks.spark.csv")
//    .option("header", "true") // Use first line of all files as header
//    .option("nullValue", "null")
//    .load("/FileStore/tables/StormEvents_details_ftp_v1_0_d2016_c20190817-4ede3.csv")
// val df_2017_storm_events = spark.read
//    .format("com.databricks.spark.csv")
//    .option("header", "true") // Use first line of all files as header
//    .option("nullValue", "null")
//    .load("/FileStore/tables/StormEvents_details_ftp_v1_0_d2017_c20200121-7c7b9.csv")
// val df_2018_storm_events = spark.read
//    .format("com.databricks.spark.csv")
//    .option("header", "true") // Use first line of all files as header
//    .option("nullValue", "null")
//    .load("/FileStore/tables/StormEvents_details_ftp_v1_0_d2018_c20200317-669e2.csv")

// COMMAND ----------

// // Creating temp tables out of dataframes created earlier
// df_2011_storm_events.select($"EPISODE_ID", $"EVENT_ID", $"STATE", $"YEAR", $"MONTH_NAME", $"EVENT_TYPE", $"CZ_TYPE",
//                             $"CZ_NAME", $"INJURIES_DIRECT", $"INJURIES_INDIRECT", $"DEATHS_DIRECT", $"DEATHS_INDIRECT", 
//                             $"DAMAGE_PROPERTY",$"DAMAGE_CROPS"
//                            ).createOrReplaceTempView("2011_storm_events");
// df_2012_storm_events.select($"EPISODE_ID", $"EVENT_ID", $"STATE", $"YEAR", $"MONTH_NAME", $"EVENT_TYPE", $"CZ_TYPE",
//                             $"CZ_NAME", $"INJURIES_DIRECT", $"INJURIES_INDIRECT", $"DEATHS_DIRECT", $"DEATHS_INDIRECT", 
//                             $"DAMAGE_PROPERTY",$"DAMAGE_CROPS"
//                            ).createOrReplaceTempView("2012_storm_events");
// df_2013_storm_events.select($"EPISODE_ID", $"EVENT_ID", $"STATE", $"YEAR", $"MONTH_NAME", $"EVENT_TYPE", $"CZ_TYPE",
//                             $"CZ_NAME", $"INJURIES_DIRECT", $"INJURIES_INDIRECT", $"DEATHS_DIRECT", $"DEATHS_INDIRECT", 
//                             $"DAMAGE_PROPERTY",$"DAMAGE_CROPS"
//                            ).createOrReplaceTempView("2013_storm_events");
// df_2014_storm_events.select($"EPISODE_ID", $"EVENT_ID", $"STATE", $"YEAR", $"MONTH_NAME", $"EVENT_TYPE", $"CZ_TYPE",
//                             $"CZ_NAME", $"INJURIES_DIRECT", $"INJURIES_INDIRECT", $"DEATHS_DIRECT", $"DEATHS_INDIRECT", 
//                             $"DAMAGE_PROPERTY",$"DAMAGE_CROPS"
//                            ).createOrReplaceTempView("2014_storm_events");
// df_2015_storm_events.select($"EPISODE_ID", $"EVENT_ID", $"STATE", $"YEAR", $"MONTH_NAME", $"EVENT_TYPE", $"CZ_TYPE",
//                             $"CZ_NAME", $"INJURIES_DIRECT", $"INJURIES_INDIRECT", $"DEATHS_DIRECT", $"DEATHS_INDIRECT", 
//                             $"DAMAGE_PROPERTY",$"DAMAGE_CROPS"
//                            ).createOrReplaceTempView("2015_storm_events");
// df_2016_storm_events.select($"EPISODE_ID", $"EVENT_ID", $"STATE", $"YEAR", $"MONTH_NAME", $"EVENT_TYPE", $"CZ_TYPE",
//                             $"CZ_NAME", $"INJURIES_DIRECT", $"INJURIES_INDIRECT", $"DEATHS_DIRECT", $"DEATHS_INDIRECT", 
//                             $"DAMAGE_PROPERTY",$"DAMAGE_CROPS"
//                            ).createOrReplaceTempView("2016_storm_events");
// df_2017_storm_events.select($"EPISODE_ID", $"EVENT_ID", $"STATE", $"YEAR", $"MONTH_NAME", $"EVENT_TYPE", $"CZ_TYPE",
//                             $"CZ_NAME", $"INJURIES_DIRECT", $"INJURIES_INDIRECT", $"DEATHS_DIRECT", $"DEATHS_INDIRECT", 
//                             $"DAMAGE_PROPERTY",$"DAMAGE_CROPS"
//                            ).createOrReplaceTempView("2017_storm_events");
// df_2018_storm_events.select($"EPISODE_ID", $"EVENT_ID", $"STATE", $"YEAR", $"MONTH_NAME", $"EVENT_TYPE", $"CZ_TYPE",
//                             $"CZ_NAME", $"INJURIES_DIRECT", $"INJURIES_INDIRECT", $"DEATHS_DIRECT", $"DEATHS_INDIRECT", 
//                             $"DAMAGE_PROPERTY",$"DAMAGE_CROPS"
//                            ).createOrReplaceTempView("2018_storm_events");

// COMMAND ----------

// // Aggregating storm events by Year and State from 2011 to 2018 and storing it together in one df
// var storms_agg_df = spark.sql("SELECT YEAR, STATE, EVENT_TYPE, COUNT(EVENT_ID) as COUNT_STORM_EVENTS, CAST(SUM(INJURIES_DIRECT) AS INT) AS TOTAL_INJURIES_DIRECT, CAST(SUM(INJURIES_INDIRECT) AS INT) AS TOTAL_INJURIES_INDIRECT, CAST(SUM(DEATHS_DIRECT) AS INT) AS TOTAL_DEATHS_DIRECT, CAST(SUM(DEATHS_INDIRECT) AS INT) AS TOTAL_DEATHS_INDIRECT, CAST(SUM(DAMAGE_PROPERTY) AS INT) AS TOTAL_DAMAGE_PROPERTY, CAST(SUM(DAMAGE_CROPS) AS INT) AS TOTAL_DAMAGE_CROPS FROM 2011_storm_events GROUP BY YEAR, STATE, EVENT_TYPE")

// for(a <- 2012 to 2018){
//   var query_str = "SELECT YEAR, STATE, EVENT_TYPE, COUNT(EVENT_ID) as COUNT_STORM_EVENTS, CAST(SUM(INJURIES_DIRECT) AS INT) AS TOTAL_INJURIES_DIRECT, CAST(SUM(INJURIES_INDIRECT) AS INT) AS TOTAL_INJURIES_INDIRECT, CAST(SUM(DEATHS_DIRECT) AS INT) AS TOTAL_DEATHS_DIRECT, CAST(SUM(DEATHS_INDIRECT) AS INT) AS TOTAL_DEATHS_INDIRECT, CAST(SUM(DAMAGE_PROPERTY) AS INT) AS TOTAL_DAMAGE_PROPERTY, CAST(SUM(DAMAGE_CROPS) AS INT) AS TOTAL_DAMAGE_CROPS FROM "+a+"_storm_events GROUP BY YEAR, STATE, EVENT_TYPE"
//   storms_agg_df = storms_agg_df.union(spark.sql(query_str))
// }
