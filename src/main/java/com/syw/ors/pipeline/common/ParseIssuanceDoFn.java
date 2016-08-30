package com.syw.ors.pipeline.common;

import java.util.List;

import org.json.simple.parser.ParseException;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.values.KV;

public class ParseIssuanceDoFn  extends DoFn<String, List<KV<String, String>>>{
	private static final long serialVersionUID = 6038769171360330879L;

	@Override
	public void processElement(ProcessContext c) throws ParseException {
		String line = c.element();
		List<KV<String, String>> record = ORSIssuanceParser.convertIssuanceToRecordMap(line);
		if(record!=null && record.size()>0){
			c.output(record);
		}
	}

	}
