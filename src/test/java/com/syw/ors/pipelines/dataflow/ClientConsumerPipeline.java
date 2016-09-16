package com.syw.ors.pipelines.dataflow;

import java.util.ArrayList;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.syw.ors.common.OrsClientRequestPutDoFn;
import com.syw.ors.common.OrsClientStringToRowConverter;
import com.syw.ors.common.OrsIssuanceSchemaCreate;
import com.syw.ors.common.OrsRequestParserTableRowDoFn;
import com.syw.ors.common.ProdConstants;

public class ClientConsumerPipeline   implements ProdConstants{
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
	    
	    	    
	
	    PCollection<String> clientRequestStr = p.apply(
    		PubsubIO.Read.named("ReadFromPubSub").topic("projects" + "/" + PROJECT_ID_PROD + "/" + "topics" + "/" + PUBSUB_CLIENT_REQUESTS_TOPIC));
    	
	    clientRequestStr.apply(
	    		ParDo.named("OrsClientPut").of(new OrsClientRequestPutDoFn()));
	    
	    PCollection<TableRow> tableRowCollection = clientRequestStr.apply(
	    		ParDo.named("convertStringtoTableRow").of(new OrsClientStringToRowConverter()));
	    
	    //private static final String DATA_TYPE_STRING = "STRING";
	    List<TableFieldSchema> FIELDS = new ArrayList<>();
	    FIELDS.add(new TableFieldSchema().setName("test").setType("STRING"));
	    TableSchema SCHEMA = new TableSchema().setFields(FIELDS);
	    
	    tableRowCollection.apply(
	    		BigQueryIO.Write
	    		 .named("WritetoBigQuery")
	    		 .to("syw-ors-1226:ors_logs.client_test_consume")
	    		 .withSchema(OrsIssuanceSchemaCreate.createSchema())
	    		 //.withoutValidation()
	    		 .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
	    		 .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
	    		 	    		 
	    p.run(); 
}

}