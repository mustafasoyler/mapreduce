package com.mapreduce.reduce;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.mapreduce.map.Mapper;
import com.mapreduce.util.MapReduceTypeEnum;

public class Reducer extends Thread {
	private String reducerName;
	private String searchItem;
	private Mapper [] mappers;
	private int sumOfCount;
	
	//Extra value for average mapreduce
	private BigDecimal average = BigDecimal.ZERO;
	
	public Reducer(String reducerName, String searchItem, Mapper [] mappers) {
		super();
		this.reducerName = reducerName;
		this.searchItem = searchItem;
		this.mappers = mappers;
	}
	
	public String getReducerName() {
		return reducerName;
	}

	public void setReducerName(String reducerName) {
		this.reducerName = reducerName;
	}

	public String getSearchItem() {
		return searchItem;
	}

	public void setSearchItem(String searchItem) {
		this.searchItem = searchItem;
	}

	public Mapper[] getMappers() {
		return mappers;
	}

	public void setMappers(Mapper[] mappers) {
		this.mappers = mappers;
	}

	public int getSumOfCount() {
		return sumOfCount;
	}

	public void setSumOfCount(int sumOfCount) {
		this.sumOfCount = sumOfCount;
	}

	public BigDecimal getAverage() {
		return average;
	}

	public void setAverage(BigDecimal average) {
		this.average = average;
	}
	
	public MapReduceTypeEnum getReducerType() {
		if(mappers != null && mappers.length > 0)
			return mappers[0].getMapperType();
		return null;
	}

	public void reduce() {
		int sumOfFrequency = 0;
		for (Mapper mapper : mappers) {
			if(MapReduceTypeEnum.COUNTER.equals(mapper.getMapperType()))
				sumOfCount += mapper.getWordCountMap().get(searchItem) == null ? 0 : mapper.getWordCountMap().get(searchItem);
			else if(MapReduceTypeEnum.AVERAGE.equals(mapper.getMapperType())) {
				sumOfCount += mapper.getLetterCountMap().get(searchItem) == null ? 0 : mapper.getLetterCountMap().get(searchItem);
				sumOfFrequency += mapper.getWordCountMap().get(searchItem) == null ? 0 : mapper.getWordCountMap().get(searchItem);
			}
		}
		
		if(sumOfFrequency > 0) {
			double averageDouble = (double)sumOfCount / sumOfFrequency;
			average = BigDecimal.valueOf(averageDouble).setScale(2, RoundingMode.HALF_UP);
		}
	}
	
	@Override
	public void run() {
		this.reduce();
	}
}
