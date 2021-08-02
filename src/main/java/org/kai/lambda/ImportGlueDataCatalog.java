// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package org.kai.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.gson.Gson;
import com.jayway.jsonpath.JsonPath;
import org.kai.util.Constants.AttributeValue;
import org.kai.util.Constants.MessageType;
import org.kai.util.GDCUtil;
import org.kai.util.TableInfo;


import java.util.*;
import java.util.stream.Collectors;

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
		String targetGlueCatalogId = Optional.ofNullable(System.getenv("target_glue_catalog_id")).orElse("1234567890");
		String S3SourceToTargetMapList = Optional.ofNullable(System.getenv("S3SourceToTargetMapList")).orElse("");
		String S3SourceToTargetMapListSeparator = Optional.ofNullable(System.getenv("S3SourceToTargetMapListSeparator")).orElse(",");
		String S3SourceToTargetMapListValuesSeparator = Optional.ofNullable(System.getenv("S3SourceToTargetMapListValuesSeparator")).orElse("|");
		boolean skipTableArchive = Boolean.parseBoolean(Optional.ofNullable(System.getenv("skip_archive")).orElse("true"));

		// Print environment variables
		printEnvVariables (region, targetGlueCatalogId, S3SourceToTargetMapList, S3SourceToTargetMapListSeparator, S3SourceToTargetMapListValuesSeparator);
		Map<String, String> s3SourceToTargetMap  = tokenizeS3SourceToTargetMapList (region, S3SourceToTargetMapList, S3SourceToTargetMapListSeparator, S3SourceToTargetMapListValuesSeparator);

		// Set client configuration
		ClientConfiguration cc = new ClientConfiguration();
		cc.setMaxErrorRetry(10);

		// Create Objects for Glue and SQS
		AWSGlue glue = AWSGlueClientBuilder.standard().withRegion(region).withClientConfiguration(cc).build();

		GDCUtil gdcUtil = new GDCUtil();


		// Process records
		/**
		 * Iterate and process all the messages which are part of SQSEvent
		 * SNS -> SQS -> Lambda is not the same as SQS -> Lambda.  Using JsonPath to pull the Type and MessageBody and Attributes.
		 */
		System.out.println("Number of messages in SQS Event: " + event.getRecords().size());
		Gson gson = new Gson();
		for (SQSEvent.SQSMessage msg : event.getRecords()) {
			String payLoad = msg.getBody();
			String internalBodyType = JsonPath.read(payLoad, "$.Type");
			String internalBodyMsg = JsonPath.read(payLoad, "$.Message");
			Map<String, HashMap> internalBodyMsgAttributes = JsonPath.read(payLoad, "$.MessageAttributes");
			System.out.println("Type: " + internalBodyType);
			System.out.println("Message: " + internalBodyMsg);
			System.out.println("MessageAttributes: " + internalBodyMsgAttributes);
			String msgExportBatchId = "";
			String msgSourceGlueCatalogId = "";
			String msgMessageType = "";

			if (!"Notification".equals(internalBodyType)) {
				System.out.println("Error: Expected SNS Type Notification.  Received: " + payLoad);
				return "Failure";
			}
			// Read SQS Message Attributes
			for (Map.Entry<String, HashMap> entry : internalBodyMsgAttributes.entrySet()) {
				if (AttributeValue.ExportBatchId.equalsIgnoreCase(entry.getKey())) {
					msgExportBatchId = (String) entry.getValue().get("Value");
					System.out.println("Export Batch Id: " + msgExportBatchId);
				} else if (AttributeValue.SourceGlueDataCatalogId.equalsIgnoreCase(entry.getKey())) {
					msgSourceGlueCatalogId = (String) entry.getValue().get("Value");
					System.out.println("Source Glue Data Catalog Id: " + msgSourceGlueCatalogId);
				} else if (AttributeValue.MessageType.equalsIgnoreCase(entry.getKey())) {
					msgMessageType = (String) entry.getValue().get("Value");
					System.out.println("Message Type " + msgMessageType);
				}
			}
			if (msgMessageType.equalsIgnoreCase(MessageType.TableInfo.toString())) {
				TableInfo tbi = gson.fromJson(internalBodyMsg, TableInfo.class);
				if (Optional.ofNullable(tbi).isPresent()) {
					System.out.println("Info: Deserialized tableinfo" + tbi.toString());
					List<Partition> partitionInfo = tbi.getPartitionList(); // prime the PartitionInfo, if necessary
					System.out.println("Info: Deserialized tablepartitioninfo" + partitionInfo);
					String s3Location = tbi.getTable().getStorageDescriptor().getLocation();
					String s3Bucket = s3Location.substring(0,ordinalIndexOf(s3Location, "/", 3));
					String s3BucketShort = s3Bucket.substring(5); // cut the s3://
					System.out.println("Source S3 Location:" + s3Location + " bucket: " + s3BucketShort);

					if (!s3SourceToTargetMap.containsKey(s3BucketShort)) {
						System.out.println("Warning: Did not find mapping from S3 Bucket " + s3BucketShort + " to local bucket. Will not copy meta");
						return "Success";
					}
					String s3TargetBucket = "s3://"+s3SourceToTargetMap.get(s3BucketShort);
					tbi.getTable().getStorageDescriptor().setLocation(s3Location.replaceFirst(s3Bucket, s3TargetBucket));
					System.out.println("Replaced Table Location " + tbi.getTable().getStorageDescriptor().getLocation());

					for (Partition p : partitionInfo) {
						String partitionLocation = p.getStorageDescriptor().getLocation();
						String partitionBucket = partitionLocation.substring(0,ordinalIndexOf(partitionLocation, "/", 3));
						String partitionBucketShort = partitionBucket.substring(5); // cut the s3://
						System.out.println("Source partition Location:" + partitionLocation + " partition Bucket: " + partitionBucketShort);
						p.getStorageDescriptor().setLocation(partitionLocation.replaceFirst(partitionBucket, s3TargetBucket ));
						System.out.println("Replaced with target partition Location: " + p.getStorageDescriptor().getLocation());
					}
					gdcUtil.processTableSchema(glue, targetGlueCatalogId, tbi.getTable(), partitionInfo,  msgExportBatchId, skipTableArchive, tbi.getRegion());


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
	 * Print environment variables
	 * @param region
	 * @param target_glue_catalog_id
	 * @param S3SourceToTargetMapList
	 * @param S3SourceToTargetMapListSeparator
	 * @param S3SourceToTargetMapListValuesSeparator
	 */
	public void printEnvVariables(String region, String target_glue_catalog_id, String S3SourceToTargetMapList, String S3SourceToTargetMapListSeparator, String S3SourceToTargetMapListValuesSeparator) {
		System.out.println("Region: " + region);
		System.out.println("Target Account: " + target_glue_catalog_id);
		System.out.println("S3SourceToTargetMapList: " + S3SourceToTargetMapList);
		System.out.println("S3SourceToTargetMapListSeparator: " + S3SourceToTargetMapListSeparator);
		System.out.println("S3SourceToTargetMapListValuesSeparator: " + S3SourceToTargetMapListValuesSeparator);

	}

	public Map<String, String> tokenizeS3SourceToTargetMapList (String region, String S3SourceToTargetMapList, String S3SourceToTargetMapListSeparator, String S3SourceToTargetMapListValuesSeparator) {
		Map<String, String> returnValue = new HashMap<>();

		List<String> mapList = Collections.list(new StringTokenizer(S3SourceToTargetMapList, S3SourceToTargetMapListSeparator)).stream()
				.map(token -> (String) token)
				.collect(Collectors.toList());
		System.out.println("Number of prefixes: " + mapList.size());
		for (String m: mapList) {
			List<String> s2t = Collections.list(new StringTokenizer(m, S3SourceToTargetMapListValuesSeparator)).stream()
					.map(token -> (String) token)
					.collect(Collectors.toList());
			if (s2t.size() != 3) {
				System.out.println("Error: Expected 3 values in formation TargetRegion|SourceS3Bucket|TargetS3Bucket.  Instead found: " + m );
			}
			if (region.equals(s2t.get(0))) {
				System.out.println("Processing: mapping for this region: " + region);
				returnValue.put(s2t.get(1), s2t.get(2)); // add to mapping
			} else {
				System.out.println("Skipping: mapping not for this region: " + region + " but for another region: " + s2t.get(0));
			}
		}
		System.out.println("returning map: " + returnValue);

		return returnValue;
	}

	public static int ordinalIndexOf(String str, String substr, int n) {
		int pos = str.indexOf(substr);
		while (--n > 0 && pos != -1)
			pos = str.indexOf(substr, pos + 1);
		return pos;
	}

	
}
