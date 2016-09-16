package com.syw.ors.pipelines.dataflow;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPut;

public class QaWebService {
	public static void main(String[] args) throws ClientProtocolException, IOException {
		URL url = new URL("https://syw-ors-qa.appspot.com");
		HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
		httpCon.setDoOutput(true);
		httpCon.setRequestMethod("PUT");
		OutputStreamWriter out = new OutputStreamWriter(
		    httpCon.getOutputStream());
		out.write("{{\"memberId\":\"7081077599007791\",\"storeId\":\"3371\",\"numberOfOffers\":3,\"maxCatalinaOffers\":1,\"scores\":[{\"rtsBUCode\":\"KAPP\",\"rankingScore\":999},{\"rtsBUCode\":\"ALL_APPAREL\",\"rankingScore\":998},{\"rtsBUCode\":\"MAPP\",\"rankingScore\":997},{\"rtsBUCode\":\"FOOTWEAR\",\"rankingScore\":996},{\"rtsBUCode\":\"WAPP\",\"rankingScore\":995},{\"rtsBUCode\":\"ALL_HOME\",\"rankingScore\":994},{\"rtsBUCode\":\"HBA_OTC\",\"rankingScore\":993},{\"rtsBUCode\":\"HF\",\"rankingScore\":992},{\"rtsBUCode\":\"TOY\",\"rankingScore\":991},{\"rtsBUCode\":\"HARD_HOME\",\"rankingScore\":990},{\"rtsBUCode\":\"FOOD_CONS\",\"rankingScore\":989},{\"rtsBUCode\":\"FSG\",\"rankingScore\":982},{\"rtsBUCode\":\"FNJL\",\"rankingScore\":981},{\"rtsBUCode\":\"HA_HE_FC\",\"rankingScore\":980},{\"rtsBUCode\":\"DIY_TOOL_AUTO\",\"rankingScore\":948},{\"rtsBUCode\":\"AUTO\",\"rankingScore\":944},{\"rtsBUCode\":\"LG\",\"rankingScore\":939},{\"rtsBUCode\":\"CE\",\"rankingScore\":938},{\"rtsBUCode\":\"ODL\",\"rankingScore\":936}],\"items\":[{\"spendBUCode\":\"HBA_OTC\",\"quantity\":1,\"unitPrice\":2.19,\"type\":\"1\"},{\"spendBUCode\":\"HBA_OTC\",\"quantity\":1,\"unitPrice\":3.29,\"type\":\"1\"},{\"spendBUCode\":\"HBA_OTC\",\"quantity\":1,\"unitPrice\":3.79,\"type\":\"1\"},{\"spendBUCode\":\"HBA_OTC\",\"quantity\":1,\"unitPrice\":0.99,\"type\":\"1\"},{\"spendBUCode\":\"HBA_OTC\",\"quantity\":1,\"unitPrice\":1.99,\"type\":\"1\"},{\"spendBUCode\":\"HBA_OTC\",\"quantity\":1,\"unitPrice\":1.99,\"type\":\"1\"}]}}}");
		out.close();
		//httpCon.getInputStream();
		

		  }
}
