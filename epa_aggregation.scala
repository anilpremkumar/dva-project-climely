// Databricks notebook source
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

var df_1 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/temp_latest.csv")
//.filter($"TEMP" > 150.0).orderBy($"TEMP" desc)
//.na.fill("0.0", Seq("TEMP"))
.select($"County_Name",$"State_Name",date_format($"year_month","yyyy") as "year_month" , $"TEMP".cast(DoubleType) as "TEMP")
.groupBy($"County_Name",$"State_Name",$"year_month").avg("TEMP")
var temp = df_1.select($"County_Name",$"State_Name",$"year_month", bround(df_1("avg(TEMP)"), 2) as "TEMP" );
temp.orderBy($"TEMP" desc).show()
//temp.select(count("year_month")).show()
//display(temp)

// COMMAND ----------

// var dp = spark.read.format("csv").option("header", "true").load("/FileStore/tables/dewpoint.csv").select($"County_Name",$"State_Name",date_format($"year_month","yyyy-MM-01") as "year_month" ,$"RH_DP")

// var gases = spark.read.format("csv").option("header", "true").load("/FileStore/tables/gases_aggregation.csv")
// var pm = spark.read.format("csv").option("header", "true").load("/FileStore/tables/pm10_and_lead_agg_merged.csv").select($"County_Name",$"State_Name",date_format($"year_month","yyyy-MM-01") as "year_month" ,$"particulates_pm10_mass_avg" as "PM10", $"lead_avg" as "LEAD")

// var aqi = spark.read.format("csv").option("header", "true").load("/FileStore/tables/AQI.csv").select($"County_Name",$"State_Name",date_format($"year_month","yyyy-MM-01") as "year_month" ,$"AQI")
// var press = spark.read.format("csv").option("header", "true").load("/FileStore/tables/pressure.csv").select($"County_Name",$"State_Name",date_format($"year_month","yyyy-MM-01") as "year_month" ,$"PRESS")
// var storm_count = spark.read.format("csv").option("header", "true").load("/FileStore/tables/storm_events_reshaped_count_events.csv")
// var storm_damage = spark.read.format("csv").option("header", "true").load("/FileStore/tables/storm_events_reshaped_total_damage.csv")
// var storm_death = spark.read.format("csv").option("header", "true").load("/FileStore/tables/storm_events_reshaped_total_deaths.csv")
// var storm_injury = spark.read.format("csv").option("header", "true").load("/FileStore/tables/storm_events_reshaped_total_injuries.csv")
// var temp = spark.read.format("csv").option("header", "true").load("/FileStore/tables/temperature.csv").select($"County_Name",$"State_Name",date_format($"year_month","yyyy-MM-01") as "year_month" ,$"TEMP")
// var wind = spark.read.format("csv").option("header", "true").load("/FileStore/tables/wind_1.csv").select($"County_Name",$"State_Name",date_format($"year_month","yyyy-MM-01") as "year_month" ,$"WIND")

var df_1 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/dewpoint.csv")
//.filter($"RH_DP".isNotNull)
//.na.fill("0.0", Seq("RH_DP"))
.select($"County_Name",$"State_Name",date_format($"year_month","yyyy") as "year_month" ,$"RH_DP".cast(DoubleType) as "RH_DP").groupBy($"County_Name",$"State_Name",$"year_month").avg()

var dp = df_1.select($"County_Name",$"State_Name",$"year_month",bround(df_1("avg(RH_DP)"), 2) as "RH_DP")
dp.select(count("year_month")).show()
//dp.show()

df_1 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/aggregated_gasses_weka.csv")
//.filter($"NO".isNotNull && $"CO".isNotNull && $"so2".isNotNull && $"Ozone".isNotNull)
// .na.fill("0.0", Seq("NO"))
// .na.fill("0.0", Seq("CO"))
// .na.fill("0.0", Seq("so2"))
// .na.fill("0.0", Seq("Ozone"))
.select($"County_Name", $"State_Name", date_format($"year_month","yyyy") as "year_month" ,  $"NO".cast(DoubleType) as "NO", $"CO".cast(DoubleType) as "CO", $"so2".cast(DoubleType) as "so2", $"Ozone".cast(DoubleType) as "Ozone")
.groupBy($"County_Name",$"State_Name",$"year_month").avg()

var gases = df_1.select($"County_Name",$"State_Name",$"year_month", bround(df_1("avg(NO)"), 2) as "NO", bround(df_1("avg(CO)"), 2) as "CO" , bround(df_1("avg(CO)"), 2) as "so2", bround(df_1("avg(Ozone)"), 2) as "Ozone")
gases.select(count("year_month")).show()
//gases.show()

df_1 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/pm10_and_lead_agg_merged_weka.csv")
//.filter($"particulates_pm10_mass_avg".isNotNull && $"lead_avg".isNotNull)
// .na.fill("0.0", Seq("particulates_pm10_mass_avg"))
// .na.fill("0.0", Seq("lead_avg"))
.select($"County_Name",$"State_Name",date_format($"year_month","yyyy") as "year_month" , $"particulates_pm10_mass_avg".cast(DoubleType) as "PM10", $"lead_avg".cast(DoubleType) as "LEAD")
.groupBy($"County_Name",$"State_Name",$"year_month").avg()

var pm =df_1.select($"County_Name",$"State_Name",$"year_month", bround(df_1("avg(PM10)"), 2) as "PM10", bround(df_1("avg(LEAD)"), 2) as "LEAD")
pm.select(count("year_month")).show()
//pm.show()

df_1 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/AQI.csv")
//.filter($"AQI".isNotNull)
//.na.fill("0.0", Seq("AQI"))
.select($"County_Name",$"State_Name",date_format($"year_month","yyyy") as "year_month" , $"AQI".cast(DoubleType) as "AQI")
.groupBy($"County_Name",$"State_Name",$"year_month").avg()
var aqi = df_1.select($"County_Name",$"State_Name",$"year_month", bround(df_1("avg(AQI)"), 2) as "AQI")
aqi.select(count("year_month")).show()
                                                                  
df_1 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/pressure.csv")
//.filter($"PRESS".isNotNull)
//.na.fill("0.0", Seq("PRESS"))
.select($"County_Name",$"State_Name",date_format($"year_month","yyyy") as "year_month" ,$"PRESS".cast(DoubleType) as "PRESS")
.groupBy($"County_Name",$"State_Name",$"year_month").avg()
var press = df_1.select($"County_Name",$"State_Name",$"year_month", bround(df_1("avg(PRESS)"), 2) as "PRESS");
press.select(count("year_month")).show()
                                                                  
df_1 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/temp_latest.csv")
//.filter($"TEMP".isNotNull)
//.na.fill("0.0", Seq("TEMP"))
.select($"County_Name",$"State_Name",date_format($"year_month","yyyy") as "year_month" , $"TEMP".cast(DoubleType) as "TEMP")
.groupBy($"County_Name",$"State_Name",$"year_month").avg()
var temp = df_1.select($"County_Name",$"State_Name",$"year_month", bround(df_1("avg(TEMP)"), 2) as "TEMP" );

temp.select(count("year_month")).show()
                                                                  
df_1 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/wind_1.csv")
//.filter($"WIND".isNotNull)
//.na.fill("0.0", Seq("WIND"))
.select($"County_Name",$"State_Name",date_format($"year_month","yyyy") as "year_month", $"WIND".cast(DoubleType) as "WIND")
.groupBy($"County_Name",$"State_Name",$"year_month").avg()
var wind = df_1.select($"County_Name",$"State_Name",$"year_month", bround(df_1("avg(WIND)"), 2) as "WIND");

wind.select(count("year_month")).show()                                                                                                                               



// COMMAND ----------

dp.createOrReplaceTempView("dewpoint");
gases.createOrReplaceTempView("gases");
pm.createOrReplaceTempView("pm");
aqi.createOrReplaceTempView("aqi");
press.createOrReplaceTempView("press");
// storm_count.createOrReplaceTempView("storm_count");
// storm_damage.createOrReplaceTempView("storm_damage");
// storm_death.createOrReplaceTempView("storm_death");
// storm_injury.createOrReplaceTempView("storm_injury");
temp.createOrReplaceTempView("temperature");
wind.createOrReplaceTempView("wind");

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM  press where press > 1000 order by press desc;

// COMMAND ----------

var comball = spark.sql("""SELECT g.County_Name,g.State_Name,g.year_month, AQI, PRESS, t.TEMP, WIND, RH_DP, g.NO, CO,so2, Ozone, PM10, LEAD  FROM gases g FULL JOIN  temperature t on (g.County_Name = t.County_Name and g.State_Name = t.State_Name and g.year_month=t.year_month) FULL JOIN  dewpoint dp on (g.County_Name = dp.County_Name and g.State_Name = dp.State_Name and g.year_month=dp.year_month) FULL JOIN  pm p on (g.County_Name = p.County_Name and g.State_Name = p.State_Name and g.year_month=p.year_month) FULL JOIN  aqi aq on (g.County_Name = aq.County_Name and g.State_Name = aq.State_Name and g.year_month=aq.year_month) FULL JOIN  press pr on (g.County_Name = pr.County_Name and g.State_Name = pr.State_Name and g.year_month=pr.year_month) FULL JOIN  wind w on (g.County_Name = w.County_Name and g.State_Name = w.State_Name and g.year_month=w.year_month)""")

// var comball = spark.sql("""SELECT g.County_Name,g.State_Name,g.year_month, AQI, PRESS, t.TEMP, RH_DP, g.NO, CO,so2, Ozone, PM10, LEAD  FROM gases g join temperature t on (g.County_Name = t.County_Name and g.State_Name = t.State_Name and g.year_month=t.year_month) join dewpoint dp on (g.County_Name = dp.County_Name and g.State_Name = dp.State_Name and g.year_month=dp.year_month) join pm p on (g.County_Name = p.County_Name and g.State_Name = p.State_Name and g.year_month=p.year_month) join aqi aq on (g.County_Name = aq.County_Name and g.State_Name = aq.State_Name and g.year_month=aq.year_month) join press pr on (g.County_Name = pr.County_Name and g.State_Name = pr.State_Name and g.year_month=pr.year_month)""")

//comball.select(count("year_month")).show()
display(comball)



// COMMAND ----------

val df_X = comball.select(upper($"County_Name") as "County_Name", upper($"State_Name") as "State_Name",$"year_month" as "year", $"AQI" , $"PRESS", $"TEMP", $"RH_DP", $"NO", $"CO", $"so2", $"Ozone", $"PM10", $"WIND", $"LEAD")
display(df_X)

// COMMAND ----------

var df = spark.read.format("csv").option("header", "true").load("/FileStore/tables/storm_events_reshaped_count_events.csv")
df = df.na.fill("0.0", Seq("Count_Event_Type_Group_Air"))
.na.fill("0.0", Seq("Count_Event_Type_Group_Earth"))
.na.fill("0.0", Seq("Count_Event_Type_Group_Fire"))
.na.fill("0.0", Seq("Count_Event_Type_Group_Water"))
//df.show()
df = df.select($"CZ_Name" as "County_Name", $"State" as "State_Name", date_format($"year_month","yyyy") as "year" ,(df("Count_Event_Type_Group_Air")+df("Count_Event_Type_Group_Earth")+df("Count_Event_Type_Group_Fire")+df("Count_Event_Type_Group_Water")) as "Disaster_Count"
)

var storm_count = df.groupBy($"County_Name",$"State_Name",$"year").sum().orderBy($"year").select($"County_Name",$"State_Name",$"year", $"sum(Disaster_Count)" as "Disasters_By_Count")

storm_count.select(count("year")).show()
storm_count.show()

df = spark.read.format("csv").option("header", "true").load("/FileStore/tables/storm_events_reshaped_total_damage.csv")
df = df.na.fill("0.0", Seq("Total_Damage_Event_Group_Air"))
.na.fill("0.0", Seq("Total_Damage_Event_Group_Earth"))
.na.fill("0.0", Seq("Total_Damage_Event_Group_Fire"))
.na.fill("0.0", Seq("Total_Damage_Event_Group_Water"))
//df.show()
df = df.select($"CZ_Name" as "County_Name", $"State" as "State_Name", date_format($"year_month","yyyy") as "year" ,(df("Total_Damage_Event_Group_Air")+df("Total_Damage_Event_Group_Earth")+df("Total_Damage_Event_Group_Fire")+df("Total_Damage_Event_Group_Water")) as "Disaster_Damage"
)

var storm_damage = df.groupBy($"County_Name",$"State_Name",$"year").sum().orderBy($"year").select($"County_Name",$"State_Name",$"year", $"sum(Disaster_Damage)" as "Disasters_By_Damage")

storm_damage.select(count("year")).show()
storm_damage.show()

df = spark.read.format("csv").option("header", "true").load("/FileStore/tables/storm_events_reshaped_total_deaths.csv")
df = df.na.fill("0.0", Seq("Total_Deaths_Event_Group_Air"))
.na.fill("0.0", Seq("Total_Deaths_Event_Group_Earth"))
.na.fill("0.0", Seq("Total_Deaths_Event_Group_Fire"))
.na.fill("0.0", Seq("Total_Deaths_Event_Group_Water"))
//df.show()
df = df.select($"CZ_Name" as "County_Name", $"State" as "State_Name", date_format($"year_month","yyyy") as "year" ,(df("Total_Deaths_Event_Group_Air")+df("Total_Deaths_Event_Group_Earth")+df("Total_Deaths_Event_Group_Fire")+df("Total_Deaths_Event_Group_Water")) as "Disaster_Deaths"
)

var storm_deaths = df.groupBy($"County_Name",$"State_Name",$"year").sum().orderBy($"year").select($"County_Name",$"State_Name",$"year", $"sum(Disaster_Deaths)" as "Disasters_By_Deaths")

storm_deaths.select(count("year")).show()
storm_deaths.show()

df = spark.read.format("csv").option("header", "true").load("/FileStore/tables/storm_events_reshaped_total_injuries.csv")
df = df.na.fill("0.0", Seq("Total_Injuries_Event_Group_Air"))
.na.fill("0.0", Seq("Total_Injuries_Event_Group_Earth"))
.na.fill("0.0", Seq("Total_Injuries_Event_Group_Fire"))
.na.fill("0.0", Seq("Total_Injuries_Event_Group_Water"))
//df.show()
df = df.select($"CZ_Name" as "County_Name", $"State" as "State_Name", date_format($"year_month","yyyy") as "year" ,(df("Total_Injuries_Event_Group_Air")+df("Total_Injuries_Event_Group_Earth")+df("Total_Injuries_Event_Group_Fire")+df("Total_Injuries_Event_Group_Water")) as "Disaster_Injuries" )

var storm_injuries = df.groupBy($"County_Name",$"State_Name",$"year").sum().orderBy($"year").select($"County_Name",$"State_Name",$"year", $"sum(Disaster_Injuries)" as "Disasters_By_Injuries")

storm_injuries.select(count("year")).show()
storm_injuries.show()

// COMMAND ----------

var X_Y_count = df_X.join(storm_count, df_X("County_Name") === storm_count("County_Name") && df_X("State_Name") === storm_count("State_Name") && df_X("year") === storm_count("year") )
.select(df_X("County_Name"),df_X("State_Name"),df_X("year"), $"AQI" , $"PRESS", $"TEMP", $"RH_DP", $"NO", $"CO", $"so2", $"Ozone", $"PM10", $"WIND", $"LEAD" , $"Disasters_By_Count")

X_Y_count.select(count("year")).show()
display(X_Y_count)

// COMMAND ----------

var X_Y_damage = df_X.join(storm_damage, df_X("County_Name") === storm_damage("County_Name") && df_X("State_Name") === storm_damage("State_Name") && df_X("year") === storm_damage("year") )
.select(df_X("County_Name"),df_X("State_Name"),df_X("year"), $"AQI" , $"PRESS", $"TEMP", $"RH_DP", $"NO", $"CO", $"so2", $"Ozone", $"PM10", $"WIND", $"LEAD" , $"Disasters_By_Damage")

X_Y_damage.select(count("year")).show()
display(X_Y_damage)

// COMMAND ----------

var X_Y_deaths = df_X.join(storm_deaths, df_X("County_Name") === storm_deaths("County_Name") && df_X("State_Name") === storm_deaths("State_Name") && df_X("year") === storm_deaths("year") )
.select(df_X("County_Name"),df_X("State_Name"),df_X("year"), $"AQI" , $"PRESS", $"TEMP", $"RH_DP", $"NO", $"CO", $"so2", $"Ozone", $"PM10", $"WIND", $"LEAD" , $"Disasters_By_Deaths")

X_Y_deaths.select(count("year")).show()
display(X_Y_deaths)

// COMMAND ----------

var X_Y_injuries = df_X.join(storm_injuries, df_X("County_Name") === storm_injuries("County_Name") && df_X("State_Name") === storm_injuries("State_Name") && df_X("year") === storm_injuries("year") )
.select(df_X("County_Name"),df_X("State_Name"),df_X("year"), $"AQI" , $"PRESS", $"TEMP", $"RH_DP", $"NO", $"CO", $"so2", $"Ozone", $"PM10", $"WIND", $"LEAD" , $"Disasters_By_Injuries")

X_Y_injuries.select(count("year")).show()
display(X_Y_injuries)
