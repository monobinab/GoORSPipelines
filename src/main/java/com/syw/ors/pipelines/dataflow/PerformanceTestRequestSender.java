package com.syw.ors.pipelines.dataflow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.syw.ors.utilities.NetClient;

public class PerformanceTestRequestSender {
    public static class SendRequests extends DoFn<TableRow, String> {
	private static final long serialVersionUID = 1L;
	String URL;

	@Override
	public void processElement(ProcessContext c) {
	    String json = (String) c.element().get("data_json");
	    // "https://syw-ors-qa.appspot.com"
	    NetClient.PutHTTPS(URL, json);
	    c.output(json);
	}

	public SendRequests(String URL) {
	    this.URL = URL;
	}
    }

    public static interface PerformanceTestRequestSenderOptions extends PipelineOptions {
	@Description("URL")
	@Default.String("https://syw-ors-qa.appspot.com")
	String getURL();

	void setURL(String URL);

	@Description("requestDate")
	@Default.String("20160816")
	String getRequestDate();

	void setRequestDate(String requestDate);
    }

    public static void main(String[] args) {
	// args[0] URL
	// <URL>
	PipelineOptionsFactory.register(PerformanceTestRequestSenderOptions.class);
	PerformanceTestRequestSenderOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PerformanceTestRequestSenderOptions.class);
	Pipeline p = Pipeline.create(options);

	p.apply(BigQueryIO.Read.named("ClientRequestsPull" + options.getRequestDate()).from("syw-ors-1226:ors_logs.ors_client_requests_" + options.getRequestDate())).apply(
		ParDo.of(new SendRequests(options.getURL())));
	p.run();
    }

}
