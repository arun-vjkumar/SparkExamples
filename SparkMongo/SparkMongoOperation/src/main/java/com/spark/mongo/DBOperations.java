package com.spark.mongo;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;

public class DBOperations {
	
	public static void insertDataToNoSqlDB(Dataset<Row> dataDF, SaveMode mode, Map<String, Object> configurations){
		
		MongoSpark.write(dataDF)
		.option("database", (String) configurations.get("database"))
		.option("collection",(String) configurations.get("collection"))
		.option("uri", (String) configurations.get("uri"))
		.mode(mode)
		.save();	
	}
	
	public static Dataset<Row> loadDataFromNoSqlDB(SparkSession sparkSession, Map<String, Object> configurations){
		Dataset<Row> dataDF= MongoSpark.read(sparkSession)
		.option("database", (String) configurations.get("database"))
		.option("collection",(String) configurations.get("collection"))
		.option("uri", (String) configurations.get("uri"))
		.load();
		return dataDF;
	}

}
