package com.mapreduce.util;

import java.io.BufferedReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {
	public static void parseAverage(String inputFilePath, Map<String, Integer> wordCountMap, 
			Map<String, Integer> letterCountMap, Map<String, String> searchItemClosingMap) throws IOException {
		if(inputFilePath == null || wordCountMap == null || letterCountMap == null || searchItemClosingMap == null)
			return;
		
		File file = new File(inputFilePath);
		FileReader fr = null;
		BufferedReader br = null;
		try {
			fr = new FileReader(file);
			br = new BufferedReader(fr);
			StringBuffer strBuff = new StringBuffer();
			while(br.ready())
				strBuff.append(br.readLine());
			byte[] strBytes = strBuff.toString().getBytes("UTF-8");
			for (Map.Entry<String, String> entry : searchItemClosingMap.entrySet()) {
				Pattern regEx = Pattern.compile(entry.getKey() + "(.+?)" + entry.getValue(), Pattern.UNICODE_CHARACTER_CLASS);
				Matcher matcher = regEx.matcher(Charset.forName("UTF-8").decode(ByteBuffer.wrap(strBytes)));
				while (matcher.find())
			        if(wordCountMap.containsKey(entry.getKey())) {
			        	wordCountMap.put(entry.getKey(), wordCountMap.get(entry.getKey()) + 1);
			        	letterCountMap.put(entry.getKey(), letterCountMap.get(entry.getKey()) + matcher.group(1).length());
			        }
	    			else {
	    				wordCountMap.put(entry.getKey(), 1);
	    				letterCountMap.put(entry.getKey(), matcher.group(1).length());
	    			}
			}
			
		} finally {
			if(fr != null)
				fr.close();
			if(br != null)
				br.close();
		}	
	}
	
	public static void parseCounter(String inputFilePath, Map<String, Integer> wordCountMap, List<String> searchItems) throws IOException {
		if(inputFilePath == null || wordCountMap == null || searchItems == null)
			return;
		
		File file = new File(inputFilePath);
		FileReader fr = null;
		BufferedReader br = null;
		try {
			fr = new FileReader(file);
			br = new BufferedReader(fr);

			Queue<Character> buffer = new LinkedList<>();
		    int c = 0;
		    char character = 0;
		    while(true) {
		    	if(buffer.isEmpty()) {
		    		if((c = br.read()) == -1)
		    			break;
		    		else if(c < 33 || c > 126)
			    		continue;
		    	} else
		    		c = buffer.poll();
		    	
		    	character = (char) c;
		    	for(String str : searchItems)
			    	if(character == str.charAt(0)) {
			    		boolean result = find(str, 1, br, buffer);
			    		if(result) {
			    			if(wordCountMap.containsKey(str))
			    				wordCountMap.put(str, wordCountMap.get(str) + 1);
			    			else
			    				wordCountMap.put(str, 1);
			    		}
			    	}
		    }
		} finally {
			if(fr != null)
				fr.close();
			if(br != null)
				br.close();
		}	      
	}
	
	
	private static boolean find(String str, int startIndex, BufferedReader br, Queue<Character> buffer) throws IOException {
		if(str.length() <= startIndex)
			return true;
		
		for (Character character : buffer) {
			if(character != str.charAt(startIndex++))
				return false;
		}
		 
		int c = 0;
		char character = 0;
		while(startIndex < str.length() && (c = br.read()) != -1) {
			character = (char) c;
			buffer.add(character);
			if(character == str.charAt(startIndex)) {
				startIndex++;
				continue;
			} else
				return false;
		}	
		
		if(startIndex >= str.length())
			return true;
		else
			return false;
	}

}
