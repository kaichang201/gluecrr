// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package org.kai.lambda;


import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishResult;
import com.google.gson.Gson;
import org.kai.util.Constants.AttributeValue;
import org.kai.util.Constants.MessageType;
import org.kai.util.SNSUtil;
import org.kai.util.TableInfo;


import java.util.Map;
import java.util.Optional;

/**
 * This class has AWS Lambda Handler method. Upon invocation, it gets an SQS message from source SQS URL, gets the message from the event.
 *
 * For each message, it takes the following actions:
 * 1. Parse the message to table
 * 2. Check if a database exist in Glue
 * 3. If exist, fetches all partitions for table
 * 4. Writes table and partition information to S3
 * 5. Sends the S3 Object key to SNS topic
 *
 */
public class PublishTableSchemaLambda implements RequestHandler<SQSEvent, Object> {

	@Override
	public String handleRequest(SQSEvent event, Context context) {
		String region = Optional.ofNullable(System.getenv("region")).orElse(Regions.US_EAST_1.getName());
		String	s3BucketName = Optional.ofNullable(System.getenv("s3_bucket_name")).orElse("S3BucketNameForGlueDataCatalogPartitions_"+region);
		String	SNSTopicARN = Optional.ofNullable(System.getenv("sns_topic_arn_publish_glue_data_catalog")).orElse("");
		printEnvVariables(region, s3BucketName, SNSTopicARN);


		AmazonSNS sns = AmazonSNSClientBuilder.standard().withRegion(region).build();
		SNSUtil snsUtil = new SNSUtil();


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

			if (msgMessageType.equalsIgnoreCase(MessageType.Table.toString())) {
				System.out.println("Received SchemaType table, body: " + payLoad);
				Table table = gson.fromJson(payLoad, Table.class);
				if (Optional.ofNullable(table).isPresent()) {
					TableInfo tbi = new TableInfo();
					tbi.setTable(table, msgSourceGlueCatalogId, s3BucketName, region);
					PublishResult publishResponse = snsUtil.publishTableInfoToSNS(sns, SNSTopicARN, tbi, msgSourceGlueCatalogId, msgExportBatchId);
					if(Optional.ofNullable(publishResponse).isPresent()) {
						System.out.println("Info: Table Schema Published to SNS Topic. Message Id: " + publishResponse.getMessageId());
					} else {
						System.out.println("Error: Table Schema Published to SNS Topic.");
					}
				} else {
					System.out.printf("There is no Table with name '%s' exist in Glue Data Catalog. Tables cannot be retrieved. \n", table.getName());
				}
			} else {
				System.out.println("Error: Expected to receive SchemaType table, instead received SchemaType: " + msgMessageType + " body " + payLoad);
			}
		}
		return "Message from SQS was processed successfully!";
	}

	/**
	 * This method prints environment variables
	 * @param s3BucketName
	 */
	public static void printEnvVariables(String region, String s3BucketName, String SNSTopicARN) {
		System.out.println("Region: " + region);
		System.out.println("S3 Bucket for storing Glue Data Catalog partition data: " + s3BucketName);
		System.out.println("SNS Topic for publishing Glue Data Catalog metadata: " + SNSTopicARN);
	}


}