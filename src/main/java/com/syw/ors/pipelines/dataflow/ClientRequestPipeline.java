package com.syw.ors.pipelines.dataflow;

import java.util.ArrayList;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.syw.ors.pipeline.common.Constants;
import com.syw.ors.pipeline.common.OrsRequestFilterPredicate;
import com.syw.ors.pipeline.common.OrsRequestParserTableRowDoFn;
import com.syw.ors.pipeline.common.ParseRequestDoFn;

//import PCollection;

public class ClientRequestPipeline implements Constants{
	
	private static final String DATA_TYPE_STRING = "STRING";
	
	@SuppressWarnings("unused")
	private interface StreamingExtractOptions 
		extends BigQueryOptions, DataflowPipelineOptions{		
	}

	public static void main(String[] args) {
		
		//create Data flow Pipeline Options 
		DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		
		options.setProject("syw-ors-qa");
		options.setStagingLocation("gs://ms_orslogs/staging");		
		options.setTempLocation("gs://ms_orslogs/temp");
		options.setStreaming(true);
		
	    // Create the Pipeline object with the options we defined above.
	    Pipeline p = Pipeline.create(options);
	    
	    //Create big table schema
	    //String schema = "version_id:STRING, start_time:STRING, insert_id:STRING, request_status:STRING, log_type:STRING, data_json:STRING, load_time:STRING";
	    List<TableFieldSchema> fields = new ArrayList<>();
	    fields.add(new TableFieldSchema(). setName(VERSION_ID).setType(DATA_TYPE_STRING));
	    fields.add(new TableFieldSchema().setName(START_TIME).setType(DATA_TYPE_STRING));
	    fields.add(new TableFieldSchema().setName(INSERT_ID).setType(DATA_TYPE_STRING));
	    fields.add(new TableFieldSchema().setName(REQUEST_STATUS).setType(DATA_TYPE_STRING));
	    fields.add(new TableFieldSchema().setName(LOG_TYPE).setType(DATA_TYPE_STRING));
	    fields.add(new TableFieldSchema().setName(DATA_JSON).setType(DATA_TYPE_STRING));
	    fields.add(new TableFieldSchema().setName(LOAD_TIME).setType(DATA_TYPE_STRING));	    
	    TableSchema schema = new TableSchema().setFields(fields);
	    
	    //Load data from file as collection
	    /*PCollection<String> rawLines = p.apply(
	    		TextIO.Read.named("ReadMyFile").from("gs://ms_orslogs/input/ors_request_consumed_datafile.txt"));*/
	    
	    PCollection<String> rawLines = p.apply(
	    		PubsubIO.Read.named("ReadFromPubSub").topic("projects/syw-ors-qa/topics/repository-changes.default"));
	    
	    //apply Pipeline transforms to parse raw lines
	    PCollection<List<KV<String, String>>> parsedRecordCollection = rawLines.apply(
	    		ParDo.named("ParseRequest").of(new ParseRequestDoFn()));
	    	    
		//filter
		PCollection<List<KV<String, String>>> filteredRecordCollections = parsedRecordCollection.apply(
				Filter.byPredicate( 
						new OrsRequestFilterPredicate()
/*					new SerializableFunction<List<KV<String, String>>, Boolean>() {
						private static final long serialVersionUID = 10000L;
						@Override
						public Boolean apply(List<KV<String, String>> input) {
							return input!=null && input.size()>0;
						}
					} 
*/				)
		);
    
		//convert to table rows
	    PCollection<TableRow> tableRowCollection = filteredRecordCollections.apply(
	    		ParDo.named("RequestParser").of(new OrsRequestParserTableRowDoFn()));
	    
	    //PCollection<String> formatOutput = filteredOutput.apply(ParDo.named("Formatted").of(new ClientRequestFormatOutput()));
	    //formatOutput.apply(TextIO.Write.named("WriteintoFile").to("gs://ms_orslogs/output/ors_request_parsedfiltered_datafile.txt"));
	    
	    tableRowCollection.apply(
	    		BigQueryIO.Write
	    		 .named("WritetoBigQuery")
	    		 .to("syw-ors-qa:ors_logs.ors_client_requests_daily_partitioned")
	    		 .withSchema(schema)
	    		 //.withoutValidation()
	    		 .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
	    		 .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
	    		 	    		 
	    p.run(); 
				
	}

}
