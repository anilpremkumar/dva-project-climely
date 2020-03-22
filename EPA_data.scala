var alldf = spark.emptyDataFrame;
for( a <- 1980 to 2019){
	println( "Value of a: " + a );
	var df = spark.read.format("csv").option("header", "true").load("./daily_44201_"+a+".csv");
	df = df.withColumn("Date_Local", (col("Date Local").cast("timestamp")))
	df = df.withColumn("Arithmetic_Mean", df("Arithmetic Mean").cast("float"));
	df = df.withColumn("County_Name", (col("County Name").cast("string")))
	df = df.withColumn("State_Name", (col("State Name").cast("string")))
	//parameter Name
	//unit of measure
	if (alldf.count == 0 ){
		alldf = df
	}else{
		alldf = alldf.union(df)
	}
}
alldf.createOrReplaceTempView("ozone_ghg_emissions");

var comb1 = spark.sql("""SELECT County_Name,State_Name,date_format(Date_Local,"yyyy-MM-01") AS year_month, avg(Arithmetic_Mean) AS values_sum FROM ozone_ghg_emissions GROUP BY County_Name,State_Name,date_format(Date_Local,"yyyy-MM-01")""")
comb1 = comb1.withColumn("year_month", (col("year_month").cast("timestamp")))
comb1.coalesce(1).write.option("header", "true").csv("mycsv.csv")

