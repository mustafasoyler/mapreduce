package com.mapreduce.main;

import java.io.FileNotFoundException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.mapreduce.map.Mapper;
import com.mapreduce.map.MapperConfiguration;
import com.mapreduce.reduce.ReducerConfiguration;
import com.mapreduce.util.MapReduceTypeEnum;

public class Main {
	private static Properties prop;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		init();
		
		//Mapreduce counter
		System.out.println("COUNTER MAPREDUCE:");
		Mapper [] mappers = MapperConfiguration.configureAndRun(prop, MapReduceTypeEnum.COUNTER);
		
		System.out.println("Reduce aþamasýna geçiliyor...");
		System.out.println();
		
		ReducerConfiguration.configureAndRun(prop, mappers);
		
		//Mapreduce average
		System.out.println("AVERAGE MAPREDUCE:");
		mappers = MapperConfiguration.configureAndRun(prop, MapReduceTypeEnum.AVERAGE);
		
		System.out.println("Reduce aþamasýna geçiliyor...");
		System.out.println();
		
		ReducerConfiguration.configureAndRun(prop, mappers);
	}
	
	public static void init() throws IOException {
		prop = new Properties();
		String propFileName = "config.properties";
		InputStream inputStream = Main.class.getClassLoader().getResourceAsStream(propFileName);
		if (inputStream != null)
			try {
				prop.load(inputStream);
			} finally {
				if(inputStream != null)
					inputStream.close();
			}
		else
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
	}
}
