import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, IntegerType}

for( a <- 2011 to 2018){
	println( "Value of a: " + a );
	val df = spark.read.format("csv").option("header", "true").load("/Users/avenkatesh/GAT/dva-climely/data/"+a+"-ghg-emissions.csv");
	df.withColumn("Total_CO2", regexp_replace(df("Total_CO2"), "\\.", ""));
	df.withColumn("Total_CO2", regexp_replace(df("Total_CO2"), ",", "."));
	df.withColumn("Total_CO2", df("Total_CO2").cast("float"));
	df.createOrReplaceTempView(a+"_ghg_emissions");
}

spark.sql("show tables").show()

spark.sql("select City,State,Sum(Total_CO2) from 2012_ghg_emissions  group by City,State").show(5)
spark.sql("select a.Total_CO2 as yr_2012, b.Total_CO2 as yr_2013,a.City,a.State from 2012_ghg_emissions a left outer join 2013_ghg_emissions b on (a.City=b.City and a.State=b.State)").show(5)