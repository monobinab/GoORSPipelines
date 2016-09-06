package com.syw.ors.pipeline.common;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.parser.ParseException;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.values.KV;

public class OrsClientStringToRowConverter  extends DoFn<String, TableRow>{
	private static final long serialVersionUID = 10000;

	public void processElement(ProcessContext c) throws ParseException {
		//String record = (String) c.element();
		//TableRow row = new TableRow();
		c.output(new TableRow().set("test", c.element()));
		;
	}	
		

}
