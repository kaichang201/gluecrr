// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package org.kai.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.kai.util.Constants.AttributeValue;
import org.kai.util.Constants.MessageType;
import org.kai.util.TableInfo;


import java.util.Map;
import java.util.Optional;

/**
 * This class has AWS Lambda Handler method. It long-polls SQS, parse the
 * message to a TableInfo type and takes one of the following actions
 * 
 * 1. Create a Table if it does not exist already
 * 2. Update a Table if it exist already
 *
 */
public class ImportGlueDataCatalog implements RequestHandler<SQSEvent, Object> {

	public Object handleRequest(SQSEvent event, Context context) {
		
		String region = Optional.ofNullable(System.getenv("region")).orElse(Regions.US_WEST_2.getName());

		// Print environment variables
		printEnvVariables( region);

		// Set client configuration
		ClientConfiguration cc = new ClientConfiguration();
		cc.setMaxErrorRetry(10);

		// Create Objects for Glue and SQS
		AWSGlue glue = AWSGlueClientBuilder.standard().withRegion(region).withClientConfiguration(cc).build();
		AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(region).withClientConfiguration(cc).build();

		// Process records
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
			if (msgMessageType.equalsIgnoreCase(MessageType.TableInfo.toString())) {
				TableInfo tbi = gson.fromJson(payLoad, TableInfo.class);
				if (Optional.ofNullable(tbi).isPresent()) {
					System.out.println("Info: Deserialized tableinfo" + tbi.toString());
					if (!tbi.isLargeTable()) {  // Regular table

					} else {  // Large Table

					}

				} else {
					System.out.println("Error: Could not deserialize payload.  Expected TableInfo, received: " + payLoad);
				}
			} else {
				System.out.println("Error: Expected to receive SchemaType tableinfo, instead received SchemaType: " + msgMessageType + " body " + payLoad);
			}
		}
		return "Success";
	}

	/**
	 * This method processes SNS event and has the business logic to import
	 * Databases and Tables to Glue Catalog
	 * @param context
	 * @param snsRecods
	 * @param glue
	 * @param sqs
	 * @param sqsQueueURL
	 * @param sqsQueueURLLargeTable
	 * @param targetGlueCatalogId
	 * @param ddbTblNameForDBStatusTracking
	 * @param ddbTblNameForTableStatusTracking
	 * @param skipTableArchive
	 * @param region
	 */
//	public void processSNSEvent(Context context, List<SNSRecord> snsRecods, AWSGlue glue, AmazonSQS sqs,
//			String sqsQueueURL, String sqsQueueURLLargeTable, String targetGlueCatalogId,
//			String ddbTblNameForDBStatusTracking, String ddbTblNameForTableStatusTracking, boolean skipTableArchive,
//			String region) {
//
//		SQSUtil sqsUtil = new SQSUtil();
//		for (SNSRecord snsRecod : snsRecods) {
//			boolean isDatabaseType = false;
//			boolean isTableType = false;
//			boolean isLargeTable = false;
//			LargeTable largeTable = null;
//			Database db = null;
//			TableWithPartitions table = null;
//			Gson gson = new Gson();
//			String message = snsRecod.getSNS().getMessage();
//			context.getLogger().log("SNS Message Payload: " + message);
//
//			// Get message attributes from the SNS Payload
//			Map<String, MessageAttribute> msgAttributeMap = snsRecod.getSNS().getMessageAttributes();
//			MessageAttribute msgTypeAttr = msgAttributeMap.get("message_type");
//			MessageAttribute sourceCatalogIdAttr = msgAttributeMap.get("source_catalog_id");
//			MessageAttribute exportBatchIdAttr = msgAttributeMap.get("export_batch_id");
//			String sourceGlueCatalogId = sourceCatalogIdAttr.getValue();
//			String exportBatchId = exportBatchIdAttr.getValue();
//			context.getLogger().log("Message Type: " + msgTypeAttr.getValue());
//			context.getLogger().log("Source Catalog Id: " + sourceGlueCatalogId);
//
//			// Serialize JSON String based on the message type
//			try {
//				if (msgTypeAttr.getValue().equalsIgnoreCase("database")) {
//					db = gson.fromJson(message, Database.class);
//					isDatabaseType = true;
//				} else if (msgTypeAttr.getValue().equalsIgnoreCase("table")) {
//					table = gson.fromJson(message, TableWithPartitions.class);
//					isTableType = true;
//				} else if (msgTypeAttr.getValue().equalsIgnoreCase("largeTable")) {
//					largeTable = gson.fromJson(message, LargeTable.class);
//					isLargeTable = true;
//				}
//			} catch (JsonSyntaxException e) {
//				System.out.println("Cannot parse SNS message to Glue Database Type.");
//				e.printStackTrace();
//			}
//
//			// Execute the business logic based on the message type
//			GDCUtil gdcUtil = new GDCUtil();
//			if (isDatabaseType) {
//				gdcUtil.processDatabseSchema(glue, sqs, targetGlueCatalogId, db, message, sqsQueueURL, sourceGlueCatalogId,
//						exportBatchId, ddbTblNameForDBStatusTracking);
//			} else if (isTableType) {
//				gdcUtil.processTableSchema(glue, sqs, targetGlueCatalogId, sourceGlueCatalogId, table, message,
//						ddbTblNameForTableStatusTracking, sqsQueueURL, exportBatchId, skipTableArchive);
//			} else if (isLargeTable) {
//				sqsUtil.sendLargeTableSchemaToSQS(sqs, sqsQueueURLLargeTable, exportBatchId, sourceGlueCatalogId,
//						message, largeTable);
//			}
//		}
//	}

	/**
	 * Print environment variables
	 * @param region
	 */
	public void printEnvVariables(String region) {
		System.out.println("Region: " + region);
	}

	
}
