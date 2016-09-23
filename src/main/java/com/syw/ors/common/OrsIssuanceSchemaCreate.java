package com.syw.ors.common;

import java.util.ArrayList;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

public class OrsIssuanceSchemaCreate implements Constants{
	private static final String DATA_TYPE_STRING = "STRING";
	
	public static final String[] schema = {
			VERSION_ID, START_TIME, INSERT_ID, REQUEST_STATUS, LOG_TYPE, LOAD_TIME, MEMBER_ID, ISSUE_TIME, 
			OFFER_CODE, OFFER_TYPE, OFFER_NAME, AMOUNT_TYPE, AMOUNT, SEC_AMOUNT_TYPE, SEC_AMOUNT, PRODUCT_GROUP,
			PRODUCT_SUB_GROUP, NUMBER_OF_OFFERS, MAX_CATALINA_OFFERS
	};
	
	//create table schema where data will be inserted
	public static TableSchema createSchema() {
		
		List<TableFieldSchema> FIELDS = new ArrayList<>();
		
		for(String fieldName : schema){
			FIELDS.add(new TableFieldSchema(). setName(fieldName).setType(DATA_TYPE_STRING));
		}
		
	    TableSchema SCHEMA = new TableSchema().setFields(FIELDS);
    
        return SCHEMA;
   }
}
