// Databricks notebook source
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

var dp = spark.read.format("csv").option("header", "true").load("/FileStore/tables/dewpoint.csv").select($"County_Name",$"State_Name",date_format($"year_month","yyyy-MM-01") as "year_month" ,$"RH_DP")

var gases = spark.read.format("csv").option("header", "true").load("/FileStore/tables/gases_aggregation.csv")
var pm = spark.read.format("csv").option("header", "true").load("/FileStore/tables/pm10_and_lead_agg_merged.csv").select($"County_Name",$"State_Name",date_format($"year_month","yyyy-MM-01") as "year_month" ,$"particulates_pm10_mass_avg" as "PM10", $"lead_avg" as "LEAD")

var aqi = spark.read.format("csv").option("header", "true").load("/FileStore/tables/AQI.csv").select($"County_Name",$"State_Name",date_format($"year_month","yyyy-MM-01") as "year_month" ,$"AQI")
var press = spark.read.format("csv").option("header", "true").load("/FileStore/tables/pressure.csv").select($"County_Name",$"State_Name",date_format($"year_month","yyyy-MM-01") as "year_month" ,$"PRESS")
var storm_count = spark.read.format("csv").option("header", "true").load("/FileStore/tables/storm_events_reshaped_count_events.csv")
var storm_damage = spark.read.format("csv").option("header", "true").load("/FileStore/tables/storm_events_reshaped_total_damage.csv")
var storm_death = spark.read.format("csv").option("header", "true").load("/FileStore/tables/storm_events_reshaped_total_deaths.csv")
var storm_injury = spark.read.format("csv").option("header", "true").load("/FileStore/tables/storm_events_reshaped_total_injuries.csv")
var temp = spark.read.format("csv").option("header", "true").load("/FileStore/tables/temperature.csv").select($"County_Name",$"State_Name",date_format($"year_month","yyyy-MM-01") as "year_month" ,$"TEMP")
var wind = spark.read.format("csv").option("header", "true").load("/FileStore/tables/wind_1.csv").select($"County_Name",$"State_Name",date_format($"year_month","yyyy-MM-01") as "year_month" ,$"WIND")


// COMMAND ----------

var dp = spark.read.format("csv").option("header", "true").load("/FileStore/tables/dewpoint.csv").select($"County_Name",$"State_Name",date_format($"year_month","yyyy-MM-01") as "year_month" ,$"RH_DP")

dp.show()
//dp.select($"County_Name",$"State_Name",date_format($"year_month","yyyy-MM-01") as "year_month" ,$"RH_DP")

// COMMAND ----------

dp.createOrReplaceTempView("dewpoint");
gases.createOrReplaceTempView("gases");
pm.createOrReplaceTempView("pm");
aqi.createOrReplaceTempView("aqi");
press.createOrReplaceTempView("press");
storm_count.createOrReplaceTempView("storm_count");
storm_damage.createOrReplaceTempView("storm_damage");
storm_death.createOrReplaceTempView("storm_death");
storm_injury.createOrReplaceTempView("storm_injury");
temp.createOrReplaceTempView("temperature");
wind.createOrReplaceTempView("wind");

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM  temperature;

// COMMAND ----------

// MAGIC %sql
// MAGIC describe press;

// COMMAND ----------


//select g.County_Name,g.State_Name,g.year_month, g.NO, CO,so2, Ozone  FROM gases g
var df = spark.sql("""select g.County_Name,g.State_Name,g.year_month, t.PRESS, g.NO, CO,so2, Ozone  FROM gases g join press t on (g.County_Name = t.County_Name and g.State_Name = t.State_Name and g.year_month =t.year_month)""")
df.show()

// COMMAND ----------


var comball = spark.sql("""SELECT g.County_Name,g.State_Name,g.year_month, AQI, PRESS, t.TEMP, WIND, RH_DP, g.NO, CO,so2, Ozone, PM10, LEAD  FROM gases g join temperature t on (g.County_Name = t.County_Name and g.State_Name = t.State_Name and g.year_month=t.year_month) join dewpoint dp on (g.County_Name = dp.County_Name and g.State_Name = dp.State_Name and g.year_month=dp.year_month) join pm p on (g.County_Name = p.County_Name and g.State_Name = p.State_Name and g.year_month=p.year_month) join aqi aq on (g.County_Name = aq.County_Name and g.State_Name = aq.State_Name and g.year_month=aq.year_month) join press pr on (g.County_Name = pr.County_Name and g.State_Name = pr.State_Name and g.year_month=pr.year_month) join wind w on (g.County_Name = w.County_Name and g.State_Name = w.State_Name and g.year_month=w.year_month)""")


// COMMAND ----------

display(comball)
