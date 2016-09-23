package com.syw.ors.pipelines.dataflow;

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;

public interface MyOptions extends DataflowPipelineOptions {
	@Description("Service Url Argument")
	@Default.String("https://syw-ors-qa.appspot.com")
	String getServiceUrl();
	void setServiceUrl(String serviceUrl);
}
