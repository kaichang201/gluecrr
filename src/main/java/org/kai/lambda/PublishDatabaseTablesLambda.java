// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package org.kai.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.gson.Gson;
import org.kai.util.*;
import org.kai.util.Constants.AttributeValue;
import org.kai.util.Constants.MessageType;

import java.util.*;

/**
 * This class has AWS Lambda Handler method. Upon invocation, it gets an SQS Message from source SQS and gets the message(s) from the event.
 * 
 * For each message, it takes the following actions:
 * 1. Parse the message to database
 * 2. Check if a database exist in Glue
 * 3. If exist, fetches all tables for the database
 * 
 * For each table, it takes the following actions:
 * 1. Convert Glue Table object to JSON String (This is a Table DDL) 
 * 2. Publish the Table DDL to an SQS Topic
 *
 */
public class PublishDatabaseTablesLambda implements RequestHandler<SQSEvent, Object> {

	@Override
	public String handleRequest(SQSEvent event, Context context) {

		String region = Optional.ofNullable(System.getenv("region")).orElse(Regions.US_EAST_1.getName());
		String sqsQueue4GlueTable = Optional.ofNullable(System.getenv("sqs_queue_url_glue_table")).orElse("");
		printEnvVariables(sqsQueue4GlueTable);

		// Client configuration
		ClientConfiguration cc = new ClientConfiguration();
		cc.setMaxErrorRetry(10);
				
		AWSGlue glue = AWSGlueClientBuilder.standard().withRegion(region).withClientConfiguration(cc).build();
		AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(region).withClientConfiguration(cc).build();

		GlueUtil glueUtil = new GlueUtil();
		SQSUtil sqsUtil = new SQSUtil();

		/**
		 * Iterate and process all the messages which are part of SQSEvent
		 */
		System.out.println("Number of messages in SQS Event: " + event.getRecords().size());
		Gson gson = new Gson();
		for (SQSEvent.SQSMessage msg : event.getRecords()) {
			String payLoad = msg.getBody();
			String msgExportBatchId = "";
			String msgSourceGlueCatalogId = "";
			String msgMessageType = "";

			// Read Message Attributes
			for (Map.Entry<String, SQSEvent.MessageAttribute> entry : msg.getMessageAttributes().entrySet()) {
				if (AttributeValue.ExportBatchId.equalsIgnoreCase(entry.getKey())) {
					msgExportBatchId = entry.getValue().getStringValue();
					System.out.println("Export Batch Id: " + msgExportBatchId);
				} else if (AttributeValue.SourceGlueDataCatalogId.equalsIgnoreCase(entry.getKey())) {
					msgSourceGlueCatalogId = entry.getValue().getStringValue();
					System.out.println("Source Glue Data Catalog Id: " + msgSourceGlueCatalogId);
				} else if (AttributeValue.MessageType.equalsIgnoreCase(entry.getKey())) {
					msgMessageType = entry.getValue().getStringValue();
					System.out.println("Message Type " + msgMessageType);
				}
			}

			if (msgMessageType.equalsIgnoreCase(MessageType.Database.toString())) {
				System.out.println("Received SchemaType database, body: " + payLoad);
				Database db = gson.fromJson(payLoad, Database.class);
				Database database = glueUtil.getDatabaseIfExist(glue, msgSourceGlueCatalogId, db);
				if (Optional.ofNullable(database).isPresent()) {
					// Get Tables for a given Database
					List<Table> dbTableList = glueUtil.getTables(glue, msgSourceGlueCatalogId, database.getName());
					sqsUtil.publishTablestoSQS(sqs, sqsQueue4GlueTable, dbTableList, msgSourceGlueCatalogId, msgExportBatchId);
				} else {
					System.out.printf("There is no Database with name '%s' exist in Glue Data Catalog. Tables cannot be retrieved. \n", db.getName());
				}
			} else {
				System.out.println("Error: Expected to receive SchemaType database, instead received SchemaType: " + msgMessageType + " body " + payLoad);
			}
		}
		return "Message from SQS was processed successfully!";
	}

	/**
	 * This method prints environment variables
	 * @param sqsQueue4GlueTables
	 */
	public static void printEnvVariables( String sqsQueue4GlueTables) {
		System.out.println("SQS URL for Glue Tables: " + sqsQueue4GlueTables);
	}


}