package com.syw.ors.pipelines.dataflow;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.syw.ors.common.OrsClientRequestPutDoFn;



public class ClientRequestServicePipeline {
	@SuppressWarnings("unused")
	private interface StreamingExtractOptions 
		extends BigQueryOptions, DataflowPipelineOptions{		
	}

	
	public static void main(String[] args) throws ConfigurationException {
		//create Data flow Pipeline Options 		
		DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		
		String fileName = "src/main/config/app.properties"; //default location of property file
		if(args.length>1){
			fileName = args[0];
		}
			
		PropertiesConfiguration configuration = new PropertiesConfiguration(fileName);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		
		String projectId = configuration.getString("PROJECT_ID");
		options.setProject(projectId);
		
		String stagingLoation = configuration.getString("STAGING_LOCATION");
		options.setStagingLocation(stagingLoation);		
		
		String tempLocation = configuration.getString("TEMP_LOCATION");
		options.setTempLocation(tempLocation);
		
		options.setStreaming(true);
		
	    // Create the Pipeline object with the options we defined above.
	    Pipeline p = Pipeline.create(options);
	    
	    
	    //publishing data_json field into the pubsub
	    String pubsubClientRequestTopic = configuration.getString("PUBSUB_CLIENT_REQUESTS_TOPIC");
	    String pubSubClientRequestTopicPath = "projects" + "/" + projectId + "/" + "topics" + "/" + pubsubClientRequestTopic;
	    
	    PCollection<String> clientRequestLines = p.apply(
	    		PubsubIO.Read.named("ReadFromPubSub").topic(pubSubClientRequestTopicPath));
	    
	    String clientRequestServiceUrl = configuration.getString("SERVICE_URL_STR");
	    
	    clientRequestLines.apply(
	    		ParDo.named("OrsClientPut").of(new OrsClientRequestPutDoFn()));
		
	    
	    
	    p.run(); 
				
	}
}
