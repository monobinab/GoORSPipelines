package com.syw.ors.pipeline.common;

import java.util.ArrayList;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

public class OrsIssuanceSchemaCreate implements ProdConstants{
	private static final String DATA_TYPE_STRING = "STRING";
	
	//create table schema where data will be inserted
	public static TableSchema createSchema() {
		
		List<TableFieldSchema> FIELDS = new ArrayList<>();
	    FIELDS.add(new TableFieldSchema(). setName(VERSION_ID).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(START_TIME).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(INSERT_ID).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(REQUEST_STATUS).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(LOG_TYPE).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(DATA_JSON).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(LOAD_TIME).setType(DATA_TYPE_STRING));	  
	    
	    FIELDS.add(new TableFieldSchema().setName(ISSUE_TYPE).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(MEMBER_ID).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(ISSUE_TIME).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(OFFER_CODE).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(OFFER_TYPE).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(OFFER_NAME).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(AMOUNT_TYPE).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(AMOUNT).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(SEC_AMOUNT_TYPE).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(SEC_AMOUNT).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(PRODUCT_GROUP).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(PRODUCT_SUB_GROUP).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(NUMBER_OF_OFFERS).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(MAX_CATALINA_OFFERS).setType(DATA_TYPE_STRING));
	    
	    TableSchema SCHEMA = new TableSchema().setFields(FIELDS);
    
        return SCHEMA;
   }
}
