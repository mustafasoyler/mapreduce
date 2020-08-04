package com.mapreduce.map;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mapreduce.util.MapReduceTypeEnum;
import com.mapreduce.util.Parser;

public class Mapper extends Thread {
	private MapReduceTypeEnum mapperType;
	private String mapperName;
	private String inputFilePath;
	private List<String> searchItems;
	private Map<String, Integer> wordCountMap;
	
	//Extra maps for average mapreduce
	Map<String, String> searchItemClosingMap;
	private Map<String, Integer> letterCountMap;
	
	public Mapper(String mapperName, String inputFilePath, List<String> searchItems) {
		super();
		this.mapperType = MapReduceTypeEnum.COUNTER;
		this.mapperName = mapperName;
		this.inputFilePath = inputFilePath;
		this.searchItems = searchItems;
		this.wordCountMap = new HashMap<>();
	}
	
	public Mapper(String mapperName, String inputFilePath, Map<String, String> searchItemClosingMap) {
		super();
		this.mapperType = MapReduceTypeEnum.AVERAGE;
		this.mapperName = mapperName;
		this.inputFilePath = inputFilePath;
		this.searchItemClosingMap = searchItemClosingMap;
		this.wordCountMap = new HashMap<>();
		this.letterCountMap = new HashMap<>();
	}

	public MapReduceTypeEnum getMapperType() {
		return mapperType;
	}

	public void setMapperType(MapReduceTypeEnum mapperType) {
		this.mapperType = mapperType;
	}

	public String getMapperName() {
		return mapperName;
	}

	public void setMapperName(String mapperName) {
		this.mapperName = mapperName;
	}

	public String getInputFilePath() {
		return inputFilePath;
	}

	public void setInputFilePath(String inputFilePath) {
		this.inputFilePath = inputFilePath;
	}

	public List<String> getSearchItems() {
		return searchItems;
	}

	public void setSearchItems(List<String> searchItems) {
		this.searchItems = searchItems;
	}

	public Map<String, Integer> getWordCountMap() {
		return wordCountMap;
	}

	public void setWordCountMap(Map<String, Integer> wordCountMap) {
		this.wordCountMap = wordCountMap;
	}

	public Map<String, String> getSearchItemClosingMap() {
		return searchItemClosingMap;
	}

	public void setSearchItemClosingMap(Map<String, String> searchItemClosingMap) {
		this.searchItemClosingMap = searchItemClosingMap;
	}

	public Map<String, Integer> getLetterCountMap() {
		return letterCountMap;
	}

	public void setLetterCountMap(Map<String, Integer> letterCountMap) {
		this.letterCountMap = letterCountMap;
	}

	@Override
	public void run() {
		try {
			if(MapReduceTypeEnum.COUNTER.equals(mapperType))
				Parser.parseCounter(inputFilePath, wordCountMap, searchItems);
			else if(MapReduceTypeEnum.AVERAGE.equals(mapperType))
				Parser.parseAverage(inputFilePath, wordCountMap, letterCountMap, searchItemClosingMap);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
