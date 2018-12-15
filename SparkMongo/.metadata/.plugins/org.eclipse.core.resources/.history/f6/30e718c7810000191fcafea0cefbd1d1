package com.spark.mongo;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LoadData {
	
	public static Dataset<Row> loadDataFromFile(SparkSession sparkSession, String format, String path, Map<String, String> options){
		return sparkSession.read()
				.format(format)
				.options(options)
				.load(path);
	}

}
