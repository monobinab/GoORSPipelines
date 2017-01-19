package com.syw.ors.pipelines.dataflow;

import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

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
import com.syw.ors.common.OrsErrorIssuanceSchemaCreate;
import com.syw.ors.common.OrsIssuanceErrorFilterPredicate;
import com.syw.ors.common.OrsIssuanceFilterPredicate;
import com.syw.ors.common.OrsIssuanceSchemaCreate;
import com.syw.ors.common.OrsRequestFilterPredicate;
import com.syw.ors.common.OrsRequestParserTableRowDoFn;
import com.syw.ors.common.OrsRequestSchemaCreate;
import com.syw.ors.common.ParseIssuanceDoFn;
import com.syw.ors.common.ParseRequestDoFn;

import com.syw.ors.pipelines.dataflow.MyOptions;



public class OrsLogsPipeline {
	@SuppressWarnings("unused")
	private interface StreamingExtractOptions 
		extends BigQueryOptions, DataflowPipelineOptions{		
	}

	
	public static void main(String[] args) throws ConfigurationException {
		//create Data flow Pipeline Options 		
		//DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		
		PipelineOptionsFactory.register(MyOptions.class);
		MyOptions options = PipelineOptionsFactory.create().as(MyOptions.class);
		
		String fileName = "src/main/config/app.properties"; //default location of property file
		if(args.length>0){
			fileName = args[0];
			System.out.println("Using Property File");
		}
			
		PropertiesConfiguration configuration = new PropertiesConfiguration(fileName);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		
		String projectId = configuration.getString("PROJECT_ID");
		options.setProject(projectId);
		
		String stagingLoation = configuration.getString("STAGING_LOCATION");
		options.setStagingLocation(stagingLoation);		
		
		String tempLocation = configuration.getString("TEMP_LOCATION");
		options.setTempLocation(tempLocation);
		
		String serviceUrl = configuration.getString("SERVICE_URL");
		options.setServiceUrl(serviceUrl);
		
		options.setStreaming(true);
		
	    // Create the Pipeline object with the options we defined above.
	    Pipeline p = Pipeline.create(options);
	    
	    String pubsubRepoTopic = configuration.getString("PUBSUB_REPOSITORY_TOPIC");
	    String pubSubReposityTopicPath = "projects" + "/" + projectId + "/" + "topics" + "/" + pubsubRepoTopic;
	    
	    PCollection<String> rawLines = p.apply(
	    		PubsubIO.Read.named("ReadFromPubSub").topic(pubSubReposityTopicPath));
	    
	    //apply Pipeline transforms to parse raw lines
	    PCollection<List<KV<String, String>>> parsedIssuanceRecordCollection = rawLines.apply(
	    		ParDo.named("ParseIssuance").of(new ParseIssuanceDoFn()));
	    	    
		//filter good records
		PCollection<List<KV<String, String>>> filteredIssuanceRecordCollections = parsedIssuanceRecordCollection.apply(
				Filter.byPredicate(new OrsIssuanceFilterPredicate()));

		//filter error records
		PCollection<List<KV<String, String>>> errorIssuanceRecordCollections = parsedIssuanceRecordCollection.apply(
				Filter.byPredicate(new OrsIssuanceErrorFilterPredicate()));
		
		//convert to table rows
	    PCollection<TableRow> issuanceTableRowCollection = filteredIssuanceRecordCollections.apply(
	    		ParDo.named("convertgoodIssuanceKVtoTableRow").of(new OrsRequestParserTableRowDoFn()));
	    
	    //convert error records to table rows
	    PCollection<TableRow> issuanceErrortableRowCollection = errorIssuanceRecordCollections.apply(
	    		ParDo.named("converterrorIssuanceKVtoTableRow").of(new OrsRequestParserTableRowDoFn()));
	   
	    String datasetId = configuration.getString("DATASET_ID");
	    String issuanceTableId = configuration.getString("ISSUANCE_TABLE_ID");	    
	    String isuanceTablePath = projectId + ":" + datasetId + "." + issuanceTableId;
	    String issuanceErrorTableId = configuration.getString("ERROR_ISSUANCE_TABLE_ID");
	    String issuanceErrorTablePath = projectId + ":" + datasetId + "." + issuanceErrorTableId;
	    
	    //insert into big query	    
	    issuanceTableRowCollection.apply(
	    		BigQueryIO.Write
	    		 .named("WriteIssuanceRecordstoBigQuery")
	    		 .to(isuanceTablePath)
	    		 .withSchema(OrsIssuanceSchemaCreate.createSchema())
	    		 .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
	    		 .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
	    
	    //insert bad records into big query	
	    issuanceErrortableRowCollection.apply(
	    		BigQueryIO.Write
	    		 .named("WriteIssuanceErrorRecordstoBigQuery")
	    		 .to(issuanceErrorTablePath)
	    		 .withSchema(OrsErrorIssuanceSchemaCreate.createSchema())
	    		 .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
	    		 .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
	    
	    //Client Request: apply Pipeline transforms to parse raw lines
	    PCollection<List<KV<String, String>>> parsedClientRequestRecordCollection = rawLines.apply(
	    		ParDo.named("ParseRequest").of(new ParseRequestDoFn()));
	    
	    PCollection<String> parsedClientRequestJsonStrCollection = rawLines.apply(
	    		ParDo.named("ParseRequestBeforePublish").of(new ClientRequestParserBeforePublishDoFn()));
	    
	    //publishing data_json field into the pubsub
	    String pubsubClientRequestTopic = configuration.getString("PUBSUB_CLIENT_REQUESTS_TOPIC");
	    String pubSubClientRequestTopicPath = "projects" + "/" + projectId + "/" + "topics" + "/" + pubsubClientRequestTopic;
	    
	    
	    parsedClientRequestJsonStrCollection.apply(
				PubsubIO.Write.named("ClientRequestOutputStream").topic(pubSubClientRequestTopicPath)); 
	    	    
		//filter
		PCollection<List<KV<String, String>>> filteredClientRequestRecordCollections = parsedClientRequestRecordCollection.apply(
				Filter.byPredicate(new OrsRequestFilterPredicate()));

   
		//convert to table rows
	    PCollection<TableRow> tableRowCollection = filteredClientRequestRecordCollections.apply(
	    		ParDo.named("ConvertClientRequestToTableRows").of(new OrsRequestParserTableRowDoFn()));

	    String clientRequestTableId = configuration.getString("REQUEST_TABLE_ID");	    
	    String clientRequestTablePath = projectId + ":" + datasetId + "." + clientRequestTableId;
	    
	    tableRowCollection.apply(
	    		BigQueryIO.Write
	    		 .named("WriteClientRequesttoBigQuery")
	    		 .to(clientRequestTablePath)
	    		 .withSchema(OrsRequestSchemaCreate.createSchema())
	    		 .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
	    		 .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	    		 );
	    
	    p.run(); 
				
	}
}
