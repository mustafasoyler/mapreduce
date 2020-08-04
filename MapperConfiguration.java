package com.mapreduce.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.mapreduce.util.Constants;
import com.mapreduce.util.MapReduceTypeEnum;

public class MapperConfiguration {
	public static Mapper [] configureAndRun(Properties prop, MapReduceTypeEnum mapperType) throws IOException, InterruptedException {		
		String inputDirectory = prop.getProperty(Constants.PROP_INPUT_DIRECTORY_PARAM);
		int numberOfInputFiles = Integer.valueOf(prop.getProperty(Constants.PROP_NUMBER_OF_INPUT_FILES_PARAM));
		int numberOfSearchItems = Integer.valueOf(prop.getProperty(Constants.PROP_NUMBER_OF_SEARCH_TAGS_PARAM));
		
		List<String> inputFilePaths = new ArrayList<String>();
		for (int i = 0; i < numberOfInputFiles; i++)
			inputFilePaths.add(inputDirectory + Constants.FILE_PATH_DELIMITER + 
					prop.getProperty(Constants.PROP_INPUT_FILE_PARAM + Constants.PROP_INPUT_FILE_ORDER_DELIMITER + (i + 1)));

		Thread [] threads = new Thread[numberOfInputFiles];
		Mapper [] mappers = new Mapper[numberOfInputFiles];
		
		if(MapReduceTypeEnum.COUNTER.equals(mapperType)) {
			List<String> searchItems =  new ArrayList<String>();
			for (int i = 0; i < numberOfSearchItems; i++)
				searchItems.add(prop.getProperty(Constants.PROP_SEARCH_TAG_PARAM + Constants.PROP_SEARCH_TAG_ORDER_DELIMITER + (i + 1)));
			for(int i = 0; i < threads.length; i++) {
				mappers[i] = new Mapper("Mapper " + (i + 1), inputFilePaths.get(i), searchItems);
				threads[i] = new Thread(mappers[i]);
				threads[i].start();
			}
		} else if(MapReduceTypeEnum.AVERAGE.equals(mapperType)) {
			Map<String, String> searchItemClosingMap =  new HashMap<String, String>();
			for (int i = 0; i < numberOfSearchItems; i++)
				searchItemClosingMap.put(prop.getProperty(Constants.PROP_SEARCH_TAG_PARAM + Constants.PROP_SEARCH_TAG_ORDER_DELIMITER + (i + 1)),
						prop.getProperty(Constants.PROP_SEARCH_CLOSING_TAG_PARAM + Constants.PROP_SEARCH_TAG_ORDER_DELIMITER + (i + 1)));
			for(int i = 0; i < threads.length; i++) {
				mappers[i] = new Mapper("Mapper " + (i + 1), inputFilePaths.get(i), searchItemClosingMap);
				threads[i] = new Thread(mappers[i]);
				threads[i].start();
			}
		}

		mappersTerminationControl(threads);
		printMappers(mappers);
		
		return mappers;
	}
	
	public static void mappersTerminationControl(Thread [] threads) {
		while(true) {
			boolean termination = true;
			for(int i = 0; i < threads.length; i++)
				if(!threads[i].getState().equals(Thread.State.TERMINATED)) {
					termination = false;
					break;
				}
			if(termination) {
				System.out.println("Tüm mapperlar için map aþamasý tamamlandý");
				break;
			}
		}
	}
	
	public static void printMappers(Mapper [] mappers) {
		for(int i = 0; i < mappers.length; i++) {
			System.out.println(mappers[i].getMapperName());
			Map<String, Integer> wordCountMap = mappers[i].getWordCountMap();
			for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
				System.out.print(entry.getKey() + "->" + entry.getValue());
				if(MapReduceTypeEnum.AVERAGE.equals(mappers[i].getMapperType()))
					System.out.println("#SUM OF LETTERS#" + mappers[i].getLetterCountMap().get(entry.getKey()));
				else
					System.out.println();
			}
			System.out.println();
		}
	}
}
