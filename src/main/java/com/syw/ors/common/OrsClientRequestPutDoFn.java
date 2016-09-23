package com.syw.ors.common;

import javax.net.ssl.HttpsURLConnection;

import com.google.api.client.http.HttpStatusCodes;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.syw.ors.pipelines.dataflow.MyOptions;
import com.syw.ors.utilities.NetClientUtil;

public class OrsClientRequestPutDoFn extends DoFn<String, String>{

	private static final long serialVersionUID = -7109272740337454373L;
	//private static final String SERVICE_URL_STR = "https://syw-ors-qa.appspot.com";

	@Override
	public void processElement(ProcessContext c) throws Exception {
		String serviceUrl = ((MyOptions) c.getPipelineOptions()).getServiceUrl();
		System.out.println("Starting to Put");
		String payloadJson = c.element();
		HttpsURLConnection conn  = NetClientUtil.PutHTTPS(serviceUrl, payloadJson);
		int status = conn.getResponseCode();
		System.out.println(status);
		if(status == HttpStatusCodes.STATUS_CODE_OK){
			c.output(payloadJson+"|"+status);
		}else{
			c.output(payloadJson+"|"+NetClientUtil.getResponseBody(conn));
		}
	}

}
