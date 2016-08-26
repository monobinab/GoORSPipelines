package com.syw.ors.pipeline.common;
import java.util.List;

import org.json.simple.parser.ParseException;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;

public class OrsRequestParserTableRowDoFn extends DoFn<List<KV<String, String>>, TableRow>{
	
	private static final long serialVersionUID = 4027341697725563566L;

	@Override
	public void processElement(ProcessContext c) throws ParseException {
		List<KV<String, String>> record = c.element();		
		TableRow row = new TableRow();
		for(KV<String, String> col: record){
			row.set(col.getKey(),col.getValue());
		}
		c.output(row);
	}	
}
