package com.syw.ors.pipeline.common;

import java.util.List;
import java.util.ArrayList;
import org.json.simple.parser.ParseException;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.values.KV;

public class OrsClientRequestToStringDoFn  extends DoFn<List<KV<String, String>>, String>{

	private static final long serialVersionUID = 10000;

	@Override
	public void processElement(ProcessContext c) throws ParseException {
		List<KV<String, String>> record = c.element();	
		
		//StringBuffer messageStr = new StringBuffer();
		/*
		for(KV<String, String> col: record){
			messageStr.set(col.getValue());
		}
		*/
		ArrayList<KV<String, String>> list = (ArrayList<KV<String, String>>) record;
		c.output(list.toString());
	}
	}	