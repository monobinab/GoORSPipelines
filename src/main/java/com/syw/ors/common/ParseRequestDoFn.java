package com.syw.ors.common;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.parser.ParseException;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseRequestDoFn extends DoFn<String, List<KV<String, String>>>{

	private static final long serialVersionUID = 6038769171360330879L;
	private static final Logger Log = LoggerFactory.getLogger(OrsRequestParser.class);

	@Override
	public void processElement(ProcessContext c) throws ParseException {
		OrsRequestParser requestParser = new OrsRequestParser();
		String line = c.element();
		if(line==null){
			return;
		}
		List<KV<String, String>> record = null;
		try{
			record = requestParser.convertRequestToRecordMap(line);
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
		if(record == null || record.isEmpty()) { return; } c.output(record);
	}

}
