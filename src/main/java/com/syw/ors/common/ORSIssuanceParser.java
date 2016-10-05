package com.syw.ors.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


//import com.google.cloud.dataflow.sdk.repackaged.com.google.cloud.bigtable.config.Logger;
import com.google.cloud.dataflow.sdk.values.KV;



public class ORSIssuanceParser implements Constants {

	//TODO Logger
	
	private JSONParser jsonParser;
	
	public ORSIssuanceParser(){
		jsonParser =  new JSONParser();
	}
	


	
	public List<List<KV<String, String>>> convertIssuanceToRecordMap(String line) throws ParseException {
		List<List<KV<String, String>>> recordListSet = new ArrayList<>(); //create another container list to keep multiple recordList out of a single input		
		List<KV<String, String>> recordList = new ArrayList<>();				
		JSONObject jsonObject;


		if (!line.contains("issuance")){
			return recordListSet;
		}

		
		jsonObject = (JSONObject) jsonParser.parse(line);

		if(jsonObject!=null){
			String insertId = null;
			if(jsonObject.containsKey("insertId")){
				insertId = (String) jsonObject.get("insertId");


				if(jsonObject!=null){
					JSONObject protoPayloadObject = jsonObject.containsKey("protoPayload")?	(JSONObject) jsonObject.get("protoPayload") : null;


					if(protoPayloadObject!=null){
						String versionId = protoPayloadObject.containsKey("versionId") ? (String) protoPayloadObject.get("versionId"):null;


						Object statusObj = protoPayloadObject.get("status");
						long requestStatus=0;
						if(statusObj instanceof Long){
							requestStatus = (Long) statusObj;
						}

						String requestStatus1 = (String) Objects.toString(requestStatus);//converting to string to put into map declared as string type


						if(protoPayloadObject!=null){
							String startTime = protoPayloadObject.containsKey("startTime") ? (String) protoPayloadObject.get("startTime") : null;

							if(protoPayloadObject!=null){
								JSONArray protoPayloadObjectline = protoPayloadObject.containsKey("line") ? (JSONArray) protoPayloadObject.get("line") : null;	

								if(protoPayloadObjectline!=null){
									for(Object object : protoPayloadObjectline){ //for each	line									
										JSONObject lineJsonObject = (JSONObject) object;
										if(lineJsonObject!=null){
											String logMessage = lineJsonObject.containsKey("logMessage") ? (String) lineJsonObject.get("logMessage") : null;//null check for logMessage before casting it to avoid null pointer exception

											recordList = new ArrayList<>(); //re-allocate to create a completely new instance for every line
											if(logMessage!=null && logMessage.contains("issuance")){
												KV<String, String> versionKV = KV.of(VERSION_ID, versionId);
												recordList.add(versionKV);

												KV<String, String> startTimeKV = KV.of(START_TIME, startTime);
												recordList.add(startTimeKV);

												KV<String, String> insertIdKV = KV.of(INSERT_ID, insertId);
												recordList.add(insertIdKV);

												KV<String, String> requestStatusKV = KV.of(REQUEST_STATUS, requestStatus1);
												recordList.add(requestStatusKV);					

												String [] strArray = logMessage.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
												//String [] strArray = logMessage.split(",");

												try{
													KV<String, String> logTypeKV = null;
													KV<String, String> memberIdKV = null;
													KV<String, String> issueTimeKV = null;
													KV<String, String> offerTypeKV = null;
													KV<String, String> offerCdKV = null;
													KV<String, String> offerNmKV = null;
													KV<String, String> amtTypKV = null;
													KV<String, String> amtKV = null;
													KV<String, String> secAmtTypKV = null;
													KV<String, String> productGrpKV=null;
													KV<String, String> secAmtKV = null;
													KV<String, String> productSubGRpKV = null;
													KV<String, String> numberOfOffersKV = null;
													KV<String, String> maxCatalinaOffersKV = null;

													if(strArray!=null && strArray.length>=1 && strArray[0]!=null){
														String logType = "";
														logType = strArray[0].trim();
														logTypeKV = KV.of(LOG_TYPE, logType);
														recordList.add(logTypeKV);
													}else{
														logTypeKV = KV.of(LOG_TYPE, null);
														recordList.add(logTypeKV);
													}

													if(strArray!=null && strArray.length>=2 && strArray[1]!=null){
														String memberId = strArray[1].trim();
														memberIdKV = KV.of(MEMBER_ID, memberId);
														recordList.add(memberIdKV);
													}else{
														memberIdKV = KV.of(MEMBER_ID, null);
														recordList.add(memberIdKV);
													}

													if(strArray!=null && strArray.length>=3 && strArray[2]!=null){
														String issueTime = strArray[2].trim();
														issueTimeKV = KV.of(ISSUE_TIME, issueTime);
														recordList.add(issueTimeKV);
													}else{
														issueTimeKV = KV.of(ISSUE_TIME, null);
														recordList.add(issueTimeKV);
													}

													if(strArray!=null && strArray.length>=4 && strArray[3]!=null){
														String offerType = strArray[3].trim();
														offerTypeKV = KV.of(OFFER_TYPE, offerType);
														recordList.add(offerTypeKV);
													}else{
														offerTypeKV = KV.of(OFFER_TYPE, null);
														recordList.add(offerTypeKV);
													}

													if (strArray!=null && strArray.length>=5 && strArray[4]!=null){
														String offerCd = strArray[4].trim();
														offerCdKV = KV.of(OFFER_CODE, offerCd);
														recordList.add(offerCdKV);
													}else{
														offerCdKV = KV.of(OFFER_CODE, null);;
														recordList.add(offerCdKV);
													}

													if (strArray!=null && strArray.length>=6 && strArray[5]!=null){
														String offerNm = strArray[5].trim();
														offerNmKV = KV.of(OFFER_NAME, offerNm);
														recordList.add(offerNmKV);
													}else{
														offerNmKV = KV.of(OFFER_NAME, null);
														recordList.add(offerNmKV);
													}

													if(strArray!=null && strArray.length>=7 && strArray[6]!=null){
														String amtTyp = strArray[6].trim();
														amtTypKV = KV.of(AMOUNT_TYPE, amtTyp);
														recordList.add(amtTypKV);
													}else{
														amtTypKV = KV.of(AMOUNT_TYPE, null);
														recordList.add(amtTypKV);
													}

													if(strArray!=null && strArray.length>=8 && strArray[7]!=null){
														String amt = strArray[7].trim();
														amtKV = KV.of(AMOUNT, amt);
														recordList.add(amtKV);
													}else{
														amtKV = KV.of(AMOUNT, null);
														recordList.add(amtKV);
													}

													if(strArray!=null && strArray.length>=9 && strArray[8]!=null){
														String secAmtTyp = strArray[8].trim();
														secAmtTypKV = KV.of(SEC_AMOUNT_TYPE, secAmtTyp);
														recordList.add(secAmtTypKV);
													}else{
														secAmtTypKV = KV.of(SEC_AMOUNT_TYPE, null);
														recordList.add(secAmtTypKV);
													}

													if(strArray!=null && strArray.length>=10 && strArray[9]!=null){
														String secAmt = strArray[9].trim();
														secAmtKV = KV.of(SEC_AMOUNT, secAmt);
														recordList.add(secAmtKV);
													}else{
														secAmtKV = KV.of(SEC_AMOUNT, null);
														recordList.add(secAmtKV);
													}

													if(strArray!=null && strArray.length>=11 && strArray[10]!=null){
														String productGrp = strArray[10].trim();
														productGrpKV = KV.of(PRODUCT_GROUP, productGrp);
														recordList.add(productGrpKV);
													}else{
														productGrpKV = KV.of(PRODUCT_GROUP, null);
														recordList.add(productGrpKV);
													}

													if(strArray!=null && strArray.length>=12 && strArray[11]!=null){
														String productSubGRp = strArray[11].trim();
														productSubGRpKV = KV.of(PRODUCT_SUB_GROUP, productSubGRp);
														recordList.add(productSubGRpKV);
													}else{
														productSubGRpKV = KV.of(PRODUCT_SUB_GROUP, null);
														recordList.add(productSubGRpKV);
													}

													if(strArray!=null && strArray.length>=13 && strArray[12]!=null){
														String numberOfOffers = strArray[12].trim();
														numberOfOffersKV = KV.of(NUMBER_OF_OFFERS, numberOfOffers);
														recordList.add(numberOfOffersKV);
													}else{
														numberOfOffersKV = KV.of(NUMBER_OF_OFFERS, null);
														recordList.add(numberOfOffersKV);
													}

													if(strArray!=null && strArray.length>=14 && strArray[13]!=null){
														String maxCatalinaOffers = strArray[13].trim();
														maxCatalinaOffersKV = KV.of(MAX_CATALINA_OFFERS, maxCatalinaOffers);
														recordList.add(maxCatalinaOffersKV);
													}else{
														maxCatalinaOffersKV = KV.of(MAX_CATALINA_OFFERS, null);
														recordList.add(maxCatalinaOffersKV);
													}

												}catch (Exception e) {
													System.out.println(line);
													continue;
												}

												long currentTime = System.currentTimeMillis();
												KV<String, String> currentTimeKV = KV.of(LOAD_TIME, Long.toString(currentTime));
												recordList.add(currentTimeKV);							
											}							

											if(recordList!=null && recordList.size()>0){
												recordListSet.add(recordList); //add recordList to main container list
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}else {
			return recordListSet;
		}

		return recordListSet;
	}
		
		
	

}
	
	

