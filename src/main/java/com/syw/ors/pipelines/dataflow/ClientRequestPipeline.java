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
import com.syw.ors.pipeline.common.OrsClientRequestToStringDoFn;
import com.syw.ors.pipeline.common.OrsRequestFilterPredicate;
import com.syw.ors.pipeline.common.OrsRequestParserTableRowDoFn;
import com.syw.ors.pipeline.common.ParseRequestDoFn;
import com.syw.ors.pipeline.common.ProdConstants;
import com.syw.ors.pipeline.common.OrsRequestSchemaCreate;

//import PCollection;

public class ClientRequestPipeline implements ProdConstants{
	
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
	    		PubsubIO.Read.named("ReadFromPubSub").topic("projects" + "/" + PROJECT_ID_PROD + "/" + "topics" + "/" + PUBSUB_TOPIC_TO_READ_PROD));
	    
	    //apply Pipeline transforms to parse raw lines
	    PCollection<List<KV<String, String>>> parsedRecordCollection = rawLines.apply(
	    		ParDo.named("ParseRequest").of(new ParseRequestDoFn()));
	    	    
		//filter
		PCollection<List<KV<String, String>>> filteredRecordCollections = parsedRecordCollection.apply(
				Filter.byPredicate( new OrsRequestFilterPredicate()));

   
		//convert to table rows
	    PCollection<TableRow> tableRowCollection = filteredRecordCollections.apply(
	    		ParDo.named("RequestParser").of(new OrsRequestParserTableRowDoFn()));
	    
	    //converting to string to publish rows
	    PCollection<String> messageStrCollection = filteredRecordCollections.apply(
	    		ParDo.named("convertKVtoString").of(new OrsClientRequestToStringDoFn()));
	    
	    //publishing rows
		messageStrCollection.apply(
				PubsubIO.Write.named("OutputStream").topic("projects" + "/" + PROJECT_ID_PROD + "/" + "topics" + "/" + PUBSUB_CLIENT_REQUESTS_TOPIC)); 
	    
	    tableRowCollection.apply(
	    		BigQueryIO.Write
	    		 .named("WritetoBigQuery")
	    		 .to(PROJECT_ID_PROD + ":" + DATASET_ID_PROD + "." + REQUEST_TABLE_ID_PROD)
	    		 .withSchema(OrsRequestSchemaCreate.createSchema())
	    		 //.withoutValidation()
	    		 .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
	    		 .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	    		 );
	    		 	    		 
	    p.run(); 
				
	}

}
