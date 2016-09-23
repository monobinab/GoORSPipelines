package com.syw.ors.common;


import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.DoFn;



public class ClientRequestParserBeforePublishDoFn extends DoFn<String, String>{
	

	private static final long serialVersionUID = 1L;
	private static final Logger Log = LoggerFactory.getLogger(ORSIssuanceParser.class);//TODO USE ERROR LOG

	@Override
	public void processElement(ProcessContext c) throws ParseException {
		ClientRequestDataJsonParserBeforePublish clientRequestDataJsonParser = new ClientRequestDataJsonParserBeforePublish();
		String line = c.element();
		if(line==null){
			return;
		}
		
		String record = null;
		
		try{
			record = clientRequestDataJsonParser.parseRequestBeforePublish(line);
		}catch (Exception e) {
			StringBuilder errMsg = new StringBuilder();			
			StackTraceElement[] stackTraceElement = e.getStackTrace();
			for(StackTraceElement element: stackTraceElement){
				errMsg.append(element.toString()).append(" ");
			}
			
			if(record == null || record.isEmpty()) { return; } c.output(record);
			}
		
	}

}
