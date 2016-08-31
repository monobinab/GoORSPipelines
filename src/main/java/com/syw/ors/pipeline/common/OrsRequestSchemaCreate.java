package com.syw.ors.pipeline.common;

import java.util.ArrayList;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.syw.ors.pipeline.common.ProdConstants;

public class OrsRequestSchemaCreate implements ProdConstants{
	
	private static final String DATA_TYPE_STRING = "STRING";
	
	
	public static TableSchema createSchema() {
		
		List<TableFieldSchema> FIELDS = new ArrayList<>();
	    FIELDS.add(new TableFieldSchema().setName(VERSION_ID).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(START_TIME).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(INSERT_ID).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(REQUEST_STATUS).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(LOG_TYPE).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(DATA_JSON).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(LOAD_TIME).setType(DATA_TYPE_STRING));	    
	    
	    TableSchema SCHEMA = new TableSchema().setFields(FIELDS);
    
        return SCHEMA;
   }
}
