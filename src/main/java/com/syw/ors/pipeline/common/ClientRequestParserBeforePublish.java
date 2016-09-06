package com.syw.ors.pipeline.common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.parser.ParseException;


public class ClientRequestParserBeforePublish  implements ProdConstants{
	
	public static void main(String[] args) throws IOException, ParseException {
		String filePath = "/Users/msaha/Documents/data/input/ors_parsedrequest_datafile.txt";
		FileReader reader = new FileReader(filePath);
		BufferedReader bufferReader = new BufferedReader(reader);						
		String line = null;			
		while( (line=bufferReader.readLine()) != null){
			//System.out.println(line);
			System.out.println(parseRequestBeforePublish(line));
		}
		bufferReader.close();
		reader.close();
}
	
	public static  String parseRequestBeforePublish(String line) throws ParseException {

		String outputStr = new String();
		if(line!=null){
				
		
			String[] recordStr = line.split("data_json");
			String data_json = recordStr[1];
			
			if(data_json!=null){
				String[] data_json1 = data_json.split("load_time");
				String data_json2 = data_json1[0];
				String data_json3 = data_json2.substring(2);
				
				if(data_json3!=null){ 
					String[] data_json4 = data_json3.split(", KV");
					String data_json5 = data_json4[0];
					outputStr = data_json5;
					return outputStr;
				}
			}
		}
		
		return outputStr;
	
	}
	
	
}
