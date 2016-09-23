package com.syw.ors.common;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.parser.ParseException;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ParseIssuanceDoFn extends DoFn<String, List<KV<String, String>>>{
	private static final long serialVersionUID = 6038769171360330879L;
    //private static ORSIssuanceParser issuanceParser = new ORSIssuanceParser();;//TODO: instantiate it inside ProcessElement
    private static final Logger Log = LoggerFactory.getLogger(ORSIssuanceParser.class);
    
	public ParseIssuanceDoFn() {
		super();
		//issuanceParser = new ORSIssuanceParser();
	}
	
	@Override
	public void processElement(ProcessContext c) throws ParseException {
		ORSIssuanceParser issuanceParser = new ORSIssuanceParser();
		String line = c.element();
		if(line==null){
			return;
		}
		
		List<List<KV<String, String>>> recordList = null;
		
		try{
			recordList = issuanceParser.convertIssuanceToRecordMap(line);
		}catch (Exception e) {
			StringBuilder errMsg = new StringBuilder();			
			StackTraceElement[] stackTraceElement = e.getStackTrace();
			for(StackTraceElement element: stackTraceElement){
				errMsg.append(element.toString()).append(" ");
			}
			
			errMsg.append(e.getMessage());
			
			List<KV<String, String>> errorRecord = new ArrayList<>();
			KV<String, String> rawDataKv = KV.of("raw_data", "RawLine:"+line);
			errorRecord.add(rawDataKv);
			KV<String, String> errKv = KV.of("error_log", "ErrorMsg:"+errMsg);			
			errorRecord.add(errKv);
			System.out.println("RawLine:"+line);
			Log.info("RawLine:"+line);
			System.out.println("ErrorMsg:"+errMsg);
			Log.error("ErrorMsg:"+errMsg);
			c.output(errorRecord);
		}
		
		if(recordList!=null && recordList.size()>0){
			for(List<KV<String, String>> record:recordList){
				c.output(record);
			}			
		}
	}

}

