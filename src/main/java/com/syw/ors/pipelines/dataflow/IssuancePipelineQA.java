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
import com.syw.ors.common.OrsErrorIssuanceSchemaCreate;
import com.syw.ors.common.OrsIssuanceErrorFilterPredicate;
import com.syw.ors.common.OrsIssuanceFilterPredicate;
import com.syw.ors.common.OrsIssuanceSchemaCreate;
import com.syw.ors.common.OrsRequestFilterPredicate;
import com.syw.ors.common.OrsRequestParserTableRowDoFn;
import com.syw.ors.common.ParseIssuanceDoFn;
import com.syw.ors.common.ProdConstants;
import com.syw.ors.common.QAConstants;

public class IssuancePipelineQA   implements ProdConstants, QAConstants{
	@SuppressWarnings("unused")
	private interface StreamingExtractOptions 
		extends BigQueryOptions, DataflowPipelineOptions{		
	}

	public static void main(String[] args) {
		
		//create Data flow Pipeline Options 
		DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		
		options.setProject(PROJECT_ID_QA);
		options.setStagingLocation(STAGING_LOCATION_QA);		
		options.setTempLocation(TEMP_LOCATION_QA);
		options.setStreaming(true);
		
	    // Create the Pipeline object with the options we defined above.
	    Pipeline p = Pipeline.create(options);
	    
	    	    
	    PCollection<String> rawLines = p.apply(
	    		PubsubIO.Read.named("ReadFromPubSub").topic("projects" + "/" + PROJECT_ID_QA + "/" + "topics" + "/" + PUBSUB_TOPIC_TO_READ_QA));
	    
	    //apply Pipeline transforms to parse raw lines
	    PCollection<List<KV<String, String>>> parsedRecordCollection = rawLines.apply(
	    		ParDo.named("ParseIssuance").of(new ParseIssuanceDoFn()));
	    	    
		//filter good records
		PCollection<List<KV<String, String>>> filteredRecordCollections = parsedRecordCollection.apply(
				Filter.byPredicate( new OrsIssuanceFilterPredicate()));

		//filter error records
		PCollection<List<KV<String, String>>> errorRecordCollections = parsedRecordCollection.apply(
				Filter.byPredicate( new OrsIssuanceErrorFilterPredicate()));
		
		//convert to table rows
	    PCollection<TableRow> tableRowCollection = filteredRecordCollections.apply(
	    		ParDo.named("convertgoodKVtoTableRow").of(new OrsRequestParserTableRowDoFn()));
	    
	  //convert error records to table rows
	    PCollection<TableRow> errortableRowCollection = errorRecordCollections.apply(
	    		ParDo.named("converterrorKVtoTableRow").of(new OrsRequestParserTableRowDoFn()));
	    
 
	    
	   //insert into big query	    
	    tableRowCollection.apply(
	    		BigQueryIO.Write
	    		 .named("WritetoBigQuery")
	    		 .to(PROJECT_ID_QA + ":" + DATASET_ID_QA + "." + ISSUANCE_TABLE_ID_QA)
	    		 .withSchema(OrsIssuanceSchemaCreate.createSchema())
	    		 //.withoutValidation()
	    		 .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
	    		 .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
	    
	  //insert bad records into big query	
	    errortableRowCollection.apply(
	    		BigQueryIO.Write
	    		 .named("WritetoBigQuery")
	    		 .to(PROJECT_ID_QA + ":" + DATASET_ID_QA + "." + ERROR_ISSUANCE_TABLE_ID_QA)
	    		 .withSchema(OrsErrorIssuanceSchemaCreate.createSchema())
	    		 //.withoutValidation()
	    		 .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
	    		 .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
	    		 	    		 
	    p.run(); 
				
	}
}
