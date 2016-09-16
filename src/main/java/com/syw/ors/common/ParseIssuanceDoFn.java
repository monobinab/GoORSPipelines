package com.syw.ors.common;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.parser.ParseException;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;

public class ParseIssuanceDoFn extends DoFn<String, List<KV<String, String>>>{
	private static final long serialVersionUID = 6038769171360330879L;
    private static ORSIssuanceParser issuanceParser = new ORSIssuanceParser();;
    
	public ParseIssuanceDoFn() {
		super();
		//issuanceParser = new ORSIssuanceParser();
	}
	
	@Override
	public void processElement(ProcessContext c) throws ParseException {
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
			System.out.println("ErrorMsg:"+errMsg);
			c.output(errorRecord);
		}
		
		if(recordList!=null && recordList.size()>0){
			for(List<KV<String, String>> record:recordList){
				c.output(record);
			}			
		}
	}

}

