package com.syw.ors.common;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.cloud.dataflow.sdk.values.KV;


public class OrsRequestParser implements Constants{
	
	private JSONParser jsonParser;
	
	public OrsRequestParser(){
		jsonParser =  new JSONParser();
	}
		
	public List<KV<String, String>> convertRequestToRecordMap(String line) throws ParseException {
		List<KV<String, String>> recordList = new ArrayList<>();
				
		JSONObject jsonObject;
		
		if (!line.contains("request")){
			return recordList;
		}
	
		jsonObject = (JSONObject) jsonParser.parse(line);
		
		//JSONObject httpRequestObject = (JSONObject) jsonObject.get("httpRequest");
		//long statusCode = (long) httpRequestObject.get("status");
		if(jsonObject!=null){
			String insertId = (String) jsonObject.get("insertId");
			JSONObject protoPayloadObject = (JSONObject) jsonObject.get("protoPayload");
			if(protoPayloadObject!=null){
				String versionId = (String) protoPayloadObject.get("versionId");
				long requestStatus = (long) protoPayloadObject.get("status");
				String requestStatus1 = (String) Objects.toString(requestStatus);//converting to string to put into map declared as string type
				String startTime = (String) protoPayloadObject.get("startTime");
		
				
				JSONArray protoPayloadObjectline = (JSONArray) protoPayloadObject.get("line");			
				if(protoPayloadObjectline!=null){
					for(Object object : protoPayloadObjectline){ //for each				
						JSONObject lineJsonObject = (JSONObject) object;
						String logMessage = (String) lineJsonObject.get("logMessage");
						//String severity = (String) lineJsonObject.get("severity");
						//String time = (String) lineJsonObject.get("time");
						//String startTime = (String) lineJsonObject.get("startTime");
				
						if(logMessage!=null && logMessage.contains("request")){
							KV<String, String> versionKV = KV.of(VERSION_ID, versionId);
							recordList.add(versionKV);
					
							KV<String, String> startTimeKV = KV.of(START_TIME, startTime);
							recordList.add(startTimeKV);
					
							KV<String, String> insertIdKV = KV.of(INSERT_ID, insertId);
							recordList.add(insertIdKV);
					
							KV<String, String> requestStatusKV = KV.of(REQUEST_STATUS, requestStatus1);
							recordList.add(requestStatusKV);
					
							String logType = "request";
							KV<String, String> logypeKV = KV.of(LOG_TYPE, logType);
							recordList.add(logypeKV);
					
							String dataJson = "";					
							String [] strArray = logMessage.split("request,");
							if(strArray!=null && strArray.length==2 && strArray[1]!=null){
								dataJson = strArray[1].trim();
								KV<String, String> dataJsonKV = KV.of(DATA_JSON, dataJson);
								recordList.add(dataJsonKV);
							}
					
							long currentTime = System.currentTimeMillis();
							KV<String, String> currentTimeKV = KV.of(LOAD_TIME, Long.toString(currentTime));
							recordList.add(currentTimeKV);
					
							//String currentUser = System.getProperty("user.name");		
							return recordList;
				}					
			}
		}
		}
		}
		//System.out.println("insertId: " + insertId + "\n" + "statusCode: " + statusCode + "\n" + "protoPayloadline: " + protoPayloadObjectline);
		return recordList;
	}
		
}

