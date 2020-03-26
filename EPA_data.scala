var alldf = spark.emptyDataFrame;
for( a <- 1980 to 2019){
	println( "Value of a: " + a );
	var df = spark.read.format("csv").option("header", "true").load("./ozone/daily_44201_"+a+".csv");
	df = df.withColumn("Date_Local", (col("Date Local").cast("timestamp")))
	df = df.withColumn("Arithmetic_Mean", df("Arithmetic Mean").cast("float"));
	df = df.withColumn("County_Name", (col("County Name").cast("string")))
	df = df.withColumn("State_Name", (col("State Name").cast("string")))
	if (alldf.count == 0 ){
		alldf = df
	}else{
		alldf = alldf.union(df)
	}
}
alldf.createOrReplaceTempView("ozone_ghg_emissions");

var comb1 = spark.sql("""SELECT County_Name,State_Name,date_format(Date_Local,"yyyy-MM-01") AS year_month, avg(Arithmetic_Mean) AS Ozone FROM ozone_ghg_emissions GROUP BY County_Name,State_Name,date_format(Date_Local,"yyyy-MM-01")""")
comb1.createOrReplaceTempView("ozone_aggregation");
comb1.coalesce(1).write.option("header", "true").csv("ozone_aggregated.csv");

var alldf = spark.emptyDataFrame;
for( a <- 1980 to 2019){
	println( "Value of a: " + a );
	var df = spark.read.format("csv").option("header", "true").load("./so2/daily_42401_"+a+".csv");
	df = df.withColumn("Date_Local", (col("Date Local").cast("timestamp")))
	df = df.withColumn("Arithmetic_Mean", df("Arithmetic Mean").cast("float"));
	df = df.withColumn("County_Name", (col("County Name").cast("string")))
	df = df.withColumn("State_Name", (col("State Name").cast("string")))
	if (alldf.count == 0 ){
		alldf = df
	}else{
		alldf = alldf.union(df)
	}
}
alldf.createOrReplaceTempView("so2_ghg_emissions");

var comb1 = spark.sql("""SELECT County_Name,State_Name,date_format(Date_Local,"yyyy-MM-01") AS year_month, avg(Arithmetic_Mean) AS so2 FROM so2_ghg_emissions GROUP BY County_Name,State_Name,date_format(Date_Local,"yyyy-MM-01")""")
comb1.createOrReplaceTempView("so2_aggregation");
comb1.coalesce(1).write.option("header", "true").csv("so2_aggregation.csv")



var alldf = spark.emptyDataFrame;
for( a <- 1980 to 2019){
	println( "Value of a: " + a );
	var df = spark.read.format("csv").option("header", "true").load("./NO/daily_42602_"+a+".csv");
	df = df.withColumn("Date_Local", (col("Date Local").cast("timestamp")))
	df = df.withColumn("Arithmetic_Mean", df("Arithmetic Mean").cast("float"));
	df = df.withColumn("County_Name", (col("County Name").cast("string")))
	df = df.withColumn("State_Name", (col("State Name").cast("string")))
	if (alldf.count == 0 ){
		alldf = df
	}else{
		alldf = alldf.union(df)
	}
}
alldf.createOrReplaceTempView("NO_ghg_emissions");

var comb1 = spark.sql("""SELECT County_Name,State_Name,date_format(Date_Local,"yyyy-MM-01") AS year_month, avg(Arithmetic_Mean) AS NO FROM NO_ghg_emissions GROUP BY County_Name,State_Name,date_format(Date_Local,"yyyy-MM-01")""")
comb1.createOrReplaceTempView("NO_aggregation");
comb1.coalesce(1).write.option("header", "true").csv("NO_aggregation.csv")


var alldf = spark.emptyDataFrame;
for( a <- 1980 to 2019){
	println( "Value of a: " + a );
	var df = spark.read.format("csv").option("header", "true").load("./CO/daily_42101_"+a+".csv");
	df = df.withColumn("Date_Local", (col("Date Local").cast("timestamp")))
	df = df.withColumn("Arithmetic_Mean", df("Arithmetic Mean").cast("float"));
	df = df.withColumn("County_Name", (col("County Name").cast("string")))
	df = df.withColumn("State_Name", (col("State Name").cast("string")))
	if (alldf.count == 0 ){
		alldf = df
	}else{
		alldf = alldf.union(df)
	}
}
alldf.createOrReplaceTempView("CO_ghg_emissions");

var comb1 = spark.sql("""SELECT County_Name,State_Name,date_format(Date_Local,"yyyy-MM-01") AS year_month, avg(Arithmetic_Mean) AS CO FROM CO_ghg_emissions GROUP BY County_Name,State_Name,date_format(Date_Local,"yyyy-MM-01")""")
comb1.createOrReplaceTempView("CO_aggregation");
comb1.coalesce(1).write.option("header", "true").csv("CO_aggregation.csv")


var df = spark.read.format("csv").option("header", "true").load("ozone_aggregated_1.csv");
df.createOrReplaceTempView("ozone_aggregation");

var df = spark.read.format("csv").option("header", "true").load("so2_aggregation_1.csv");
df.createOrReplaceTempView("so2_aggregation");

var df = spark.read.format("csv").option("header", "true").load("NO_aggregation_1.csv");
df.createOrReplaceTempView("NO_aggregation");

var df = spark.read.format("csv").option("header", "true").load("CO_aggregation_1.csv");
df.createOrReplaceTempView("CO_aggregation");


var comball = spark.sql("""SELECT no.County_Name,co.State_Name,co.year_month, NO, CO,so2, Ozone FROM NO_aggregation no join CO_aggregation co on (no.County_Name = co.County_Name and no.State_Name = co.State_Name and no.year_month=co.year_month) join so2_aggregation so on (no.County_Name = so.County_Name and no.State_Name = so.State_Name and no.year_month=so.year_month) join ozone_aggregation oz on (no.County_Name = oz.County_Name and no.State_Name = oz.State_Name and no.year_month=oz.year_month)""")


comball.coalesce(1).write.option("header", "true").csv("aggregated.csv")