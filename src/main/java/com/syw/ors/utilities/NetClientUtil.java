package com.syw.ors.utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.security.cert.Certificate;
import java.io.*;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLPeerUnverifiedException;

public class NetClientUtil {

	/**
	 * @param url_str
	 */
	public static void GetHTTPS(String url_str) {
		URL url;
		try {

			url = new URL(url_str);
			HttpsURLConnection con = (HttpsURLConnection) url.openConnection();

			print_https_cert(con);

			//print_content(con);

		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param url_str
	 * @param json
	 * @throws IOException 
	 */
	public static HttpsURLConnection PutHTTPS(String url_str, String json) throws IOException {
		URL url;
		url = new URL(url_str);
		HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
		con.setDoOutput(true);
		con.setRequestMethod("PUT");
		OutputStreamWriter out = new OutputStreamWriter(con.getOutputStream());
		out.write(json);
		out.close();
		return con;		
	}

	
	/**
	 * @param con
	 */
	private static void print_https_cert(HttpsURLConnection con) {
		if (con != null) {
			try {
				System.out.println("Response Code : " + con.getResponseCode());
				System.out.println("Cipher Suite : " + con.getCipherSuite());
				System.out.println("\n");

				Certificate[] certs = con.getServerCertificates();
				for (Certificate cert : certs) {
					System.out.println("Cert Type : " + cert.getType());
					System.out.println("Cert Hash Code : " + cert.hashCode());
					System.out.println("Cert Public Key Algorithm : "
							+ cert.getPublicKey().getAlgorithm());
					System.out.println("Cert Public Key Format : "
							+ cert.getPublicKey().getFormat());
					System.out.println("\n");
				}
			} catch (SSLPeerUnverifiedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static String getResponseBody(HttpsURLConnection con) throws IOException {
		StringBuffer buffer = new StringBuffer();		
		if (con != null) {
			BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String line = null;
			while ((line = br.readLine()) != null) {
				System.out.println(line);
				buffer.append(line).append("\t");
			}
			br.close();
		}		
		return buffer.toString();
	}
	
	
	
	

}