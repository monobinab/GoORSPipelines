package com.syw.ors.common;

import java.util.ArrayList;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

public class OrsErrorIssuanceSchemaCreate implements ProdConstants{
	private static final String DATA_TYPE_STRING = "STRING";
	
	
	public static TableSchema createSchema() {
		List<TableFieldSchema> FIELDS = new ArrayList<>();
	    FIELDS.add(new TableFieldSchema().setName(RAW_DATA).setType(DATA_TYPE_STRING));
	    FIELDS.add(new TableFieldSchema().setName(ERROR_LOG).setType(DATA_TYPE_STRING));
	    
	    TableSchema SCHEMA = new TableSchema().setFields(FIELDS);
	    
        return SCHEMA;
	}
}
