package com.syw.ors.pipelines.dataflow;

import java.util.List;

import com.google.api.services.bigquery.model.TableRow;
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
import com.syw.ors.common.ClientRequestParserBeforePublishDoFn;
import com.syw.ors.common.OrsClientRequestPutDoFn;
import com.syw.ors.common.OrsRequestFilterPredicate;
import com.syw.ors.common.OrsRequestParserTableRowDoFn;
import com.syw.ors.common.OrsRequestSchemaCreate;
import com.syw.ors.common.ParseRequestDoFn;
import com.syw.ors.common.Constants;

//import PCollection;


public class ClientRequestPipeline implements Constants{
	
	//private static final String DATA_TYPE_STRING = "STRING";
	
	@SuppressWarnings("unused")
	private interface StreamingExtractOptions 
		extends BigQueryOptions, DataflowPipelineOptions{		
	}

	public static void main(String[] args) {
		
		//create Data flow Pipeline Options 
		DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		
		options.setProject(PROJECT_ID_PROD);
		options.setStagingLocation(STAGING_LOCATION_PROD);		
		options.setTempLocation(TEMP_LOCATION_PROD);
		options.setStreaming(true);
		
	    // Create the Pipeline object with the options we defined above.
	    Pipeline p = Pipeline.create(options);
	        
	    PCollection<String> rawLines = p.apply(
	    		PubsubIO.Read.named("ReadFromPubSub").topic(PUBSUB_REPOSITORY_TOPIC_PATH));
	    
	    //apply Pipeline transforms to parse raw lines
	    PCollection<List<KV<String, String>>> parsedRecordCollection = rawLines.apply(
	    		ParDo.named("ParseRequest").of(new ParseRequestDoFn()));
	    	    
		//filter
		PCollection<List<KV<String, String>>> filteredRecordCollections = parsedRecordCollection.apply(
				Filter.byPredicate(new OrsRequestFilterPredicate()));

   
		//convert to table rows
	    PCollection<TableRow> tableRowCollection = filteredRecordCollections.apply(
	    		ParDo.named("ConvertToTableRows").of(new OrsRequestParserTableRowDoFn()));
	    
	    /*
	    //converting to string to publish rows
	    PCollection<String> messageStrCollection = filteredRecordCollections.apply(
	    		ParDo.named("convertKVtoString").of(new OrsClientRequestToStringDoFn()));
	    */
	    
	    PCollection<String> parsedmessageStrCollection = rawLines.apply(
	    		ParDo.named("ParseRequestBeforePublish").of(new ClientRequestParserBeforePublishDoFn()));
	    
	    
	   /*
	    //publishing rows
	    parsedmessageStrCollection.apply(
				PubsubIO.Write.named("OutputStream").topic(PUBSUB_CLIENT_REQUESTS_TOPIC_PATH)); 
	    
	    
	    PCollection<String> requestRawLines = p.apply(
	    		PubsubIO.Read.named("ReadFromPubSub").topic(PUBSUB_CLIENT_REQUESTS_TOPIC_PATH));
	    
	    */
	    parsedmessageStrCollection.apply(
	    		ParDo.named("OrsClientPut").of(new OrsClientRequestPutDoFn()));
	    
	    tableRowCollection.apply(
	    		BigQueryIO.Write
	    		 .named("WritetoBigQuery")
	    		 .to(REQUEST_TABLE_PATH)
	    		 .withSchema(OrsRequestSchemaCreate.createSchema())
	    		 .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
	    		 .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	    		 );
	    		 	    		 
	    p.run(); 
				
	}

}
