package com.syw.ors.pipeline.common;


import org.json.simple.parser.ParseException;

import com.google.cloud.dataflow.sdk.transforms.DoFn;



public class ClientRequestParserBeforePublishDoFn   extends DoFn<String, String>{
	private static final long serialVersionUID = 6038769171360330879L;

	@Override
	public void processElement(ProcessContext c) throws ParseException {
		String line = c.element();
		String record = ClientRequestParserBeforePublish.parseRequestBeforePublish(line);
		/*
		if(record!=null && record.size()>0){
			c.output(record);
		}
		*/
		c.output(record);
	}

}
