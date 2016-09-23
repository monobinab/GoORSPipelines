package com.syw.ors.common;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.cloud.dataflow.sdk.values.KV;


public class ClientRequestDataJsonParserBeforePublish implements Constants{
	
	private JSONParser jsonParser ;
	
	public ClientRequestDataJsonParserBeforePublish(){
		jsonParser =  new JSONParser();
	}
	
		
	public String parseRequestBeforePublish(String line) throws ParseException {
		String record = null;
				
		JSONObject jsonObject;
		
		if (!line.contains("request")){
			return record;
		}
		
		jsonObject = (JSONObject) jsonParser.parse(line);
		
		//JSONObject httpRequestObject = (JSONObject) jsonObject.get("httpRequest");
		//long statusCode = (long) httpRequestObject.get("status");
		if(jsonObject!=null){
			
			JSONObject protoPayloadObject = (JSONObject) jsonObject.get("protoPayload");
			if(protoPayloadObject!=null){
					
				
				JSONArray protoPayloadObjectline = (JSONArray) protoPayloadObject.get("line");			
				if(protoPayloadObjectline!=null){
					for(Object object : protoPayloadObjectline){ //for each				
						JSONObject lineJsonObject = (JSONObject) object;
						String logMessage = (String) lineJsonObject.get("logMessage");
						//String severity = (String) lineJsonObject.get("severity");
						//String time = (String) lineJsonObject.get("time");
						//String startTime = (String) lineJsonObject.get("startTime");
				
						if(logMessage!=null && logMessage.contains("request")){
												
							String dataJson = "";					
							String [] strArray = logMessage.split("request,");
							if(strArray!=null && strArray.length==2 && strArray[1]!=null){
								dataJson = strArray[1].trim();
								record = dataJson;
							}
					
										
							//String currentUser = System.getProperty("user.name");		
							return record;
				}					
			}
		}
		}
		}
		
		return record;
	}
		
}

