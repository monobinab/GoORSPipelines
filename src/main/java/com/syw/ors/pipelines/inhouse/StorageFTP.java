package com.syw.ors.pipelines.inhouse;

//[START all]
/*
 * Copyright (c) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.sql.SQLException;

import com.syw.ors.utilities.*;

/**
 * Main class for the Cloud Storage JSON API sample.
 *
 * Demonstrates how to make an authenticated API call using the Google Cloud
 * Storage API client library for java, with Application Default Credentials.
 */
public class StorageFTP {

    public static void main(String[] args) {
	if (args.length < 3) {
	    System.err.println("Usage: <bucket name> <https url> <QA or PROD>");
	    System.exit(1);
	}
	String bucketName = args[0];
	String url = args[1];
	String env = args[2];
	try {
	    if (env.equalsIgnoreCase("PROD")) {
		uploadAndPublish(bucketName, url, true);
	    } else if (env.equalsIgnoreCase("QA")) {
		uploadAndPublish(bucketName, url, false);
	    } else {
		System.out.println("Wrong Env! \n Usage: <bucket name> <https url> <QA or PROD>");
	    }

	} catch (IOException e) {
	    System.err.println(e.getMessage());
	    System.exit(1);
	} catch (RuntimeException e) {
	    System.err.println(e.getMessage());
	    System.exit(1);
	} catch (GeneralSecurityException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	    System.exit(1);
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	    System.exit(1);
	}
    }

    private static void uploadAndPublish(String bucketName, String url, boolean isProd) throws IOException, GeneralSecurityException, SQLException {
	String local_filename;
	local_filename = MySQLConnector.getInstance().generateFile(isProd);
	File initialFile = new File(local_filename);
	if (initialFile.exists() && !initialFile.isDirectory() && initialFile.length() > 0) {
	    InputStream targetStream = new FileInputStream(initialFile);
	    // Upload a stream to the bucket. This could very well be a file.
	    uploadStream(local_filename, "text/csv", targetStream, bucketName);
	    NetClient.GetHTTPS(url);
	}
    }

    // [START upload_stream]
    /**
     * Uploads data to an object in a bucket.
     *
     * @param name
     *            the name of the destination object.
     * @param contentType
     *            the MIME type of the data.
     * @param stream
     *            the data - for instance, you can use a FileInputStream to
     *            upload a file.
     * @param bucketName
     *            the name of the bucket to create the object in.
     */
    private static void uploadStream(String name, String contentType, InputStream stream, String bucketName) throws IOException, GeneralSecurityException {
	InputStreamContent contentStream = new InputStreamContent(contentType, stream);
	StorageObject objectMetadata = new StorageObject()
	// Set the destination object name
		.setName(name);
	// Set the access control list to publicly read-only

	// Do the insert
	Storage client = StorageFactory.getService();
	Storage.Objects.Insert insertRequest = client.objects().insert(bucketName, objectMetadata, contentStream);

	insertRequest.execute();
    }
}
// [END all]
