package com.syw.ors.pipelines.dataflow;

import java.util.List;

import org.mortbay.jetty.Main;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.syw.ors.common.OrsClientRequestPutDoFn;
import com.syw.ors.common.ParseIssuanceDoFn;
import com.syw.ors.common.ProdConstants;
import com.syw.ors.common.QAConstants;

public class OrsClientPutToQAPipeline implements ProdConstants{
	public static void main(String[] args) {
		DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		
		options.setProject(PROJECT_ID_PROD);
		options.setStagingLocation(STAGING_LOCATION_PROD);		
		options.setTempLocation(TEMP_LOCATION_PROD);
		options.setStreaming(true);
		
	    // Create the Pipeline object with the options we defined above.
	    Pipeline p = Pipeline.create(options);
	    
	        
	    //PCollection<String> rawLines = p.apply(
	    		//PubsubIO.Read.named("ReadFromPubSub").topic("projects" + "/" + PROJECT_ID_PROD + "/" + "topics" + "/" + PUBSUB_CLIENT_REQUESTS_TOPIC));
	    
	    PCollection<String> rawLines = p.apply(
	    		PubsubIO.Read.named("ReadFromPubSub").topic("projects" + "/" + PROJECT_ID_PROD + "/" + "topics" + "/" + PUBSUB_CLIENT_REQUESTS_TOPIC));
	  
	    //apply Pipeline transforms to parse raw lines
	    rawLines.apply(
	    		ParDo.named("OrsClientPut").of(new OrsClientRequestPutDoFn()));
	    
	    p.run();
	    
	    
	}
}
