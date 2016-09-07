package com.syw.ors.common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.cloud.dataflow.sdk.values.KV;

public class ORSIssuanceParser implements ProdConstants {

	private static JSONParser jsonParser =  new JSONParser();
	
	public static void main(String[] args) throws IOException, ParseException {
			String filePath = "/Users/msaha/Documents/data/input/00:00:00_00:59:59_A0:1461373200.json";
			FileReader reader = new FileReader(filePath);
			BufferedReader bufferReader = new BufferedReader(reader);						
			String line = null;			
			while( (line=bufferReader.readLine()) != null){
				System.out.println(convertIssuanceToRecordMap(line));
			}
			bufferReader.close();
			reader.close();
	}
	
	
	public static List<KV<String, String>> convertIssuanceToRecordMap(String line) throws ParseException {
		List<KV<String, String>> recordList = new ArrayList<>();
		/*if(line == null || line.trim().isEmpty()){
			return recordList;
		}*/
		
		JSONObject jsonObject;
		
		try{
			jsonObject = (JSONObject) jsonParser.parse(line);
		}catch(Exception e){
			System.out.println("line: " + line);
			System.out.println("Error: " + e.getMessage());
			
			return recordList;
		}
		
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
				
						if(logMessage!=null && logMessage.contains("issuance")){
							KV<String, String> versionKV = KV.of(VERSION_ID, versionId);
							recordList.add(versionKV);
					
							KV<String, String> startTimeKV = KV.of(START_TIME, startTime);
							recordList.add(startTimeKV);
					
							KV<String, String> insertIdKV = KV.of(INSERT_ID, insertId);
							recordList.add(insertIdKV);
					
							KV<String, String> requestStatusKV = KV.of(REQUEST_STATUS, requestStatus1);
							recordList.add(requestStatusKV);
					
												
							//String dataJson = "";					
							String [] strArray = logMessage.split(",");
							//System.out.println(Arrays.toString(strArray)) ;
							//if(strArray.length==14){
							try{
								//dataJson = strArray[1].trim();
								//System.out.println("Reached inside loop");
								String logType = "";
								logType = strArray[0].trim();
								KV<String, String> logTypeKV = KV.of(LOG_TYPE, logType);
								recordList.add(logTypeKV);
								
								String memberId = strArray[1].trim();
								KV<String, String> memberIdKV = KV.of(MEMBER_ID, memberId);
								recordList.add(memberIdKV);
								
								String issueTime = strArray[2].trim();
								KV<String, String> issueTimeKV = KV.of(ISSUE_TIME, issueTime);
								recordList.add(issueTimeKV);
								
								String offerType = strArray[3].trim();
								KV<String, String> offerTypeKV = KV.of(OFFER_TYPE, offerType);
								recordList.add(offerTypeKV);
								
								String offerCd = strArray[4].trim();
								KV<String, String> offerCdKV = KV.of(OFFER_CODE, offerCd);
								recordList.add(offerCdKV);
								
								String offerNm = strArray[5].trim();
								KV<String, String> offerNmKV = KV.of(OFFER_NAME, offerNm);
								recordList.add(offerNmKV);
								
								String amtTyp = strArray[6].trim();
								KV<String, String> amtTypKV = KV.of(AMOUNT_TYPE, amtTyp);
								recordList.add(amtTypKV);
								
								String amt = strArray[7].trim();
								KV<String, String> amtKV = KV.of(AMOUNT, amt);
								recordList.add(amtKV);
								
								String secAmtTyp = strArray[8].trim();
								KV<String, String> secAmtTypKV = KV.of(SEC_AMOUNT_TYPE, secAmtTyp);
								recordList.add(secAmtTypKV);
								
								String secAmt = strArray[9].trim();
								KV<String, String> secAmtKV = KV.of(SEC_AMOUNT, secAmt);
								recordList.add(secAmtKV);
								
								String productGrp = strArray[10].trim();
								KV<String, String> productGrpKV = KV.of(PRODUCT_GROUP, productGrp);
								recordList.add(productGrpKV);
								
								String productSubGRp = strArray[11].trim();
								KV<String, String> productSubGRpKV = KV.of(PRODUCT_SUB_GROUP, productSubGRp);
								recordList.add(productSubGRpKV);
								
								String numberOfOffers = strArray[12].trim();
								KV<String, String> numberOfOffersKV = KV.of(NUMBER_OF_OFFERS, numberOfOffers);
								recordList.add(numberOfOffersKV);
								
								String maxCatalinaOffers = strArray[13].trim();
								KV<String, String> maxCatalinaOffersKV = KV.of(MAX_CATALINA_OFFERS, maxCatalinaOffers);
								recordList.add(maxCatalinaOffersKV);
							}catch (Exception e)
							{
							//System.out.println("Error: " + e.getMessage());
							continue;
							}
							//return recordList;
							
					
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
