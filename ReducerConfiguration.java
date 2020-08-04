package com.mapreduce.reduce;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.mapreduce.map.Mapper;
import com.mapreduce.util.Constants;
import com.mapreduce.util.MapReduceTypeEnum;

public class ReducerConfiguration {
	public static void configureAndRun(Properties prop, Mapper [] mappers) {
		int numberOfSearchItems = Integer.valueOf(prop.getProperty(Constants.PROP_NUMBER_OF_SEARCH_TAGS_PARAM));
		List<String> searchItems =  new ArrayList<String>();
		for (int i = 0; i < numberOfSearchItems; i++)
			searchItems.add(prop.getProperty(Constants.PROP_SEARCH_TAG_PARAM + Constants.PROP_SEARCH_TAG_ORDER_DELIMITER + (i + 1)));
		
		Thread [] threads = new Thread[numberOfSearchItems];
		Reducer [] reducers = new Reducer[numberOfSearchItems];

		for(int i = 0; i < threads.length; i++) {
			reducers[i] = new Reducer("Reducer " + (i + 1), searchItems.get(i), mappers);
			threads[i] = new Thread(reducers[i]);
			threads[i].start();
		}
		
		reducersTerminationControl(threads);
		printReducers(reducers);
	}
	
	public static void reducersTerminationControl(Thread [] threads) {
		while(true) {
			boolean termination = true;
			for(int i = 0; i < threads.length; i++)
				if(!threads[i].getState().equals(Thread.State.TERMINATED)) {
					termination = false;
					break;
				}
			if(termination) {
				System.out.println("Tüm reducerlar için reduce aþamasý tamamlandý");
				break;
			}
		}
	}
	
	public static void printReducers(Reducer [] reducers) {
		for(int i = 0; i < reducers.length; i++) {
			System.out.println(reducers[i].getReducerName());
			if(MapReduceTypeEnum.COUNTER.equals(reducers[i].getReducerType()))
				System.out.println(reducers[i].getSearchItem() + "->" + reducers[i].getSumOfCount());
			else
				System.out.println(reducers[i].getSearchItem() + "->" + reducers[i].getAverage());
			System.out.println();
		}
	}
}
