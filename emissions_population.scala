// Databricks notebook source
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, IntegerType}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._

// val schema = new StructType()
//   .add("Year",StringType,true)
//   .add("Total_CO2e",IntegerType,true)
//   .add("City",StringType,true)
// .add("State",StringType,true)
// .add("Zip_Code",StringType,true)

// COMMAND ----------

val df1 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/2011_ghg_emissions-1c9d0.csv").withColumn("Total_CO2",regexp_replace($"Total_CO2",",",""))
df1.select($"Year", $"City",$"State",$"Total_CO2").createOrReplaceTempView("2011_emissions");
 val df2 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/2012_ghg_emissions-473bd.csv").withColumn("Total_CO2",regexp_replace($"Total_CO2",",",""))
df2.select($"Year", $"City",$"State",$"Total_CO2").createOrReplaceTempView("2012_emissions");
 val df3 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/2014_ghg_emissions-b780c.csv").withColumn("Total_CO2",regexp_replace($"Total_CO2",",",""))
df3.select($"Year", $"City",$"State",$"Total_CO2").createOrReplaceTempView("2013_emissions");
 val df4 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/2013_ghg_emissions-05122.csv").withColumn("Total_CO2",regexp_replace($"Total_CO2",",",""))
df4.select($"Year", $"City",$"State",$"Total_CO2").createOrReplaceTempView("2014_emissions");
 val df5 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/2015_ghg_emissions-a85ec.csv").withColumn("Total_CO2",regexp_replace($"Total_CO2",",",""))
df5.select($"Year", $"City",$"State",$"Total_CO2").createOrReplaceTempView("2015_emissions");
 val df6 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/2016_ghg_emissions-85f0e.csv").withColumn("Total_CO2",regexp_replace($"Total_CO2",",",""))
df6.select($"Year", $"City",$"State",$"Total_CO2").createOrReplaceTempView("2016_emissions");
 val df7 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/2017_ghg_emissions-90e39.csv").withColumn("Total_CO2",regexp_replace($"Total_CO2",",",""))
df7.select($"Year", $"City",$"State",$"Total_CO2").createOrReplaceTempView("2017_emissions");
 val df8 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/2018_ghg_emissions-298a3.csv").withColumn("Total_CO2",regexp_replace($"Total_CO2",",",""))
df8.select($"Year", $"City",$"State",$"Total_CO2").createOrReplaceTempView("2018_emissions");


// COMMAND ----------

spark.sql("show tables").show()
spark.sql("select * from 2016_emissions").show(5)

// COMMAND ----------


var result_df = spark.sql("select Year, City,State,Sum(Total_CO2) as Total_CO2 from 2011_emissions group by City,State,Year having State is not null")

for( a <- 2012 to 2018){
  var query_str = "select Year, City,State,Sum(Total_CO2) as Total_CO2 from "+a+"_emissions group by City,State,Year having State is not null"
  result_df = result_df.union(spark.sql(query_str))
}
//Just to test
result_df.filter($"City" === "Menifee").show()

// COMMAND ----------

var state_map =
    """[{"Full_Name" : "Alabama", "Abbr" : "AL"},
    {"Full_Name" : "Alaska", "Abbr" : "AK"},
    {"Full_Name" : "Arizona", "Abbr" : "AZ"},
    {"Full_Name" : "Arkansas", "Abbr" : "AR"},
    {"Full_Name" : "California", "Abbr" : "CA"},
    {"Full_Name" : "Colorado", "Abbr" : "CO"},
    {"Full_Name" : "Connecticut", "Abbr" : "CT"},
    {"Full_Name" : "Delaware", "Abbr" : "DE"},
    {"Full_Name" : "District of Columbia", "Abbr" : "DC"},
    {"Full_Name" : "Florida", "Abbr" : "FL"},
    {"Full_Name" : "Georgia", "Abbr" : "GA"},
    {"Full_Name" : "Hawaii", "Abbr" : "HI"},
    {"Full_Name" : "Idaho", "Abbr" : "ID"},
    {"Full_Name" : "Illinois", "Abbr" : "IL"},
    {"Full_Name" : "Indiana", "Abbr" : "IN"},
    {"Full_Name" : "Iowa", "Abbr" : "IA"},
    {"Full_Name" : "Kansas", "Abbr" : "KS"},
    {"Full_Name" : "Kentucky", "Abbr" : "KY"},
    {"Full_Name" : "Louisiana", "Abbr" : "LA"},
    {"Full_Name" : "Maine", "Abbr" : "ME"},
    {"Full_Name" : "Maryland", "Abbr" : "MD"},
    {"Full_Name" : "Massachusetts", "Abbr" : "MA"},
    {"Full_Name" : "Michigan", "Abbr" : "MI"},
    {"Full_Name" : "Minnesota", "Abbr" : "MN"},
    {"Full_Name" : "Mississippi", "Abbr" : "MS"},
    {"Full_Name" : "Missouri", "Abbr" : "MO"},
    {"Full_Name" : "Montana", "Abbr" : "MT"},
    {"Full_Name" : "Nebraska", "Abbr" : "NE"},
    {"Full_Name" : "Nevada", "Abbr" : "NV"},
    {"Full_Name" : "New Hampshire", "Abbr" : "NH"},
    {"Full_Name" : "New Jersey", "Abbr" : "NJ"},
    {"Full_Name" : "New Mexico", "Abbr" : "NM"},
    {"Full_Name" : "New York", "Abbr" : "NY"},
    {"Full_Name" : "North Carolina", "Abbr" : "NC"},
    {"Full_Name" : "North Dakota", "Abbr" : "ND"},
    {"Full_Name" : "Northern Mariana Islands", "Abbr" : "MP"},
    {"Full_Name" : "Ohio", "Abbr" : "OH"},
    {"Full_Name" : "Oklahoma", "Abbr" : "OK"},
    {"Full_Name" : "Oregon", "Abbr" : "OR"},
    {"Full_Name" : "Palau", "Abbr" : "PW"},
    {"Full_Name" : "Pennsylvania", "Abbr" : "PA"},
    {"Full_Name" : "Puerto Rico", "Abbr" : "PR"},
    {"Full_Name" : "Rhode Island", "Abbr" : "RI"},
    {"Full_Name" : "South Carolina", "Abbr" : "SC"},
    {"Full_Name" : "South Dakota", "Abbr" : "SD"},
    {"Full_Name" : "Tennessee", "Abbr" : "TN"},
    {"Full_Name" : "Texas", "Abbr" : "TX"},
    {"Full_Name" : "Utah", "Abbr" : "UT"},
    {"Full_Name" : "Vermont", "Abbr" : "VT"},
    {"Full_Name" : "Virgin Islands", "Abbr" : "VI"},
    {"Full_Name" : "Virginia", "Abbr" : "VA"},
    {"Full_Name" : "Washington", "Abbr" : "WA"},
    {"Full_Name" : "West Virginia", "Abbr" : "WV"},
    {"Full_Name" : "Wisconsin", "Abbr" : "WI"},
    {"Full_Name" : "Wyoming", "Abbr" : "WY"}]"""
val state_df = spark.read.json(Seq(state_map).toDS)
state_df.show(10)

// COMMAND ----------

 var pop_df = spark.read.format("csv").option("header", "true").load("/FileStore/tables/population.csv")
.withColumn("Name",regexp_replace($"Name"," city",""))
.withColumn("Name",regexp_replace($"Name"," town",""))
.withColumn("Name",regexp_replace($"Name"," County",""))
.withColumn("Name",regexp_replace($"Name","Balance of ",""))


// COMMAND ----------

var result_pop_df = pop_df.withColumn("Year",lit("2010")).withColumn("Population", pop_df("POPESTIMATE2010").cast("float"))
        .select($"Year", $"Name" as "City", $"STNAME" as "State", $"Population")
for( a <- 2011 to 2018){
result_pop_df = result_pop_df.union(pop_df.withColumn("Year",lit(a)).withColumn("Population", pop_df("POPESTIMATE"+a.toString).cast("float"))
        .select($"Year", $"Name" as "City", $"STNAME" as "State", $"Population"))
}
result_pop_df = result_pop_df.groupBy($"Year",$"State",$"City").sum("Population").select($"Year",$"State",$"City", $"sum(Population)" as "Total_Population")

//test - start
//result_pop_df.filter($"City".contains("Menifee")).orderBy($"Year").show(9)
//test - end

result_pop_df = result_pop_df.join(state_df,state_df("Full_Name") === result_pop_df("State")).select($"Year",$"City", $"Abbr" as "State", $"Total_Population")

result_pop_df.show()


// COMMAND ----------

result_pop_df.join(result_df, result_df("Year") === result_pop_df("Year") && result_df("State") === result_pop_df("State") && result_df("City") === result_pop_df("City"))
.select(result_df("Year"),result_df("State"),result_df("City"),$"Total_Population",$"Total_CO2").show()

//result_pop_df.join(result_df, result_df("State") === result_pop_df("State")).select(result_df("Year"),result_pop_df("State")).show()

