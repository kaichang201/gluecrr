// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package org.kai.util;

import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.google.gson.Gson;
import org.kai.util.Constants.AttributeValue;
import org.kai.util.Constants.MessageType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQSUtil {


	public void sendTableSchemaToDeadLetterQueue(AmazonSQS sqs, String queueUrl, TableReplicationStatus tableStatus,
			String exportBatchId, String sourceGlueCatalogId) {

		int statusCode = 400;
		Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
		messageAttributes.put("ExportBatchId",
				new MessageAttributeValue().withDataType("String.ExportBatchId").withStringValue(exportBatchId));
		messageAttributes.put("SourceGlueDataCatalogId", new MessageAttributeValue()
				.withDataType("String.SourceGlueDataCatalogId").withStringValue(sourceGlueCatalogId));
		messageAttributes.put("SchemaType",
				new MessageAttributeValue().withDataType("String.SchemaType").withStringValue("Table"));

		SendMessageRequest req = new SendMessageRequest().withQueueUrl(queueUrl)
				.withMessageBody(tableStatus.getTableSchema()).withMessageAttributes(messageAttributes);

		try {
			SendMessageResult sendMsgRes = sqs.sendMessage(req);
			statusCode = sendMsgRes.getSdkHttpMetadata().getHttpStatusCode();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception thrown while writing message to SQS. " + e.getLocalizedMessage());
		}
		if (statusCode == 200)
			System.out.printf("Table schema for table '%s' of database '%s' sent to SQS. \n",
					tableStatus.getTableName(), tableStatus.getDbName());

	}

	public void sendDatabaseSchemaToDeadLetterQueue(AmazonSQS sqs, String queueUrl, String databaseDDL,
			String databaseName, String exportBatchId, String sourceGlueCatalogId) {

		int statusCode = 400;
		Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
		messageAttributes.put("ExportBatchId",
				new MessageAttributeValue().withDataType("String.ExportBatchId").withStringValue(exportBatchId));
		messageAttributes.put("SourceGlueDataCatalogId", new MessageAttributeValue()
				.withDataType("String.SourceGlueDataCatalogId").withStringValue(sourceGlueCatalogId));
		messageAttributes.put("SchemaType",
				new MessageAttributeValue().withDataType("String.SchemaType").withStringValue("Database"));

		SendMessageRequest req = new SendMessageRequest().withQueueUrl(queueUrl).withMessageBody(databaseDDL)
				.withMessageAttributes(messageAttributes);

		try {
			SendMessageResult sendMsgRes = sqs.sendMessage(req);
			statusCode = sendMsgRes.getSdkHttpMetadata().getHttpStatusCode();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception thrown while writing message to SQS. " + e.getLocalizedMessage());
		}
		if (statusCode == 200)
			System.out.printf("Database schema for database '%s' sent to SQS. \n", databaseName);
	}

	/**
	 * This method publishes all Database Schemas (DDL) to SQS
	 *
	 * @param sqs
 	 * @param queueUrl
	 * @param masterDBList
	 * @param sourceGlueCatalogId
	 * @return
	 */

	public void publishDatabasesToSQS(AmazonSQS sqs, String queueUrl, List<Database> masterDBList, String sourceGlueCatalogId) {

		int statusCode = 400;
		MessageAttributeValue exportBatchIdAV =  createStringAttribute(Long.toString(System.currentTimeMillis()));
		MessageAttributeValue sourceGlueCatalogIdAV = createStringAttribute(sourceGlueCatalogId);
		MessageAttributeValue messageTypeAV = createStringAttribute(MessageType.Database.toString());
		Gson gson = new Gson();

		for (Database db : masterDBList) {
			String databaseDDL = gson.toJson(db);
			Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
			messageAttributes.put(AttributeValue.ExportBatchId, exportBatchIdAV);
			messageAttributes.put(AttributeValue.SourceGlueDataCatalogId, sourceGlueCatalogIdAV);
			messageAttributes.put(AttributeValue.MessageType, messageTypeAV);

			SendMessageRequest req = new SendMessageRequest().withQueueUrl(queueUrl)
					.withMessageBody(databaseDDL).withMessageAttributes(messageAttributes);
			try {
				SendMessageResult sendMsgRes = sqs.sendMessage(req);
				statusCode = sendMsgRes.getSdkHttpMetadata().getHttpStatusCode();
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Exception thrown while writing message to SQS. " + e.getLocalizedMessage());
			}
			if (statusCode == 200)
				System.out.printf("Database '%s' sent to SQS. \n", databaseDDL);
		}
	}

	/**
	 * This method publishes all Tables Schemas (DDL) to SQS
	 *
	 * @param sqs
	 * @param queueUrl
	 * @param masterTableList
	 * @param sourceGlueCatalogId
	 * @return
	 */
	public void publishTablestoSQS(AmazonSQS sqs, String queueUrl, List<Table> masterTableList, String sourceGlueCatalogId, String exportBatchId ) {

		int statusCode = 400;
		MessageAttributeValue exportBatchIdAV =  createStringAttribute(exportBatchId);
		MessageAttributeValue sourceGlueCatalogIdAV = createStringAttribute(sourceGlueCatalogId);
		MessageAttributeValue schemaTypeAV = createStringAttribute(MessageType.Table.toString());
		Gson gson = new Gson();

		for (Table tb : masterTableList) {
			String tableDDL = gson.toJson(tb);
			Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
			messageAttributes.put(AttributeValue.ExportBatchId, exportBatchIdAV);
			messageAttributes.put(AttributeValue.SourceGlueDataCatalogId, sourceGlueCatalogIdAV);
			messageAttributes.put(AttributeValue.MessageType, schemaTypeAV);

			SendMessageRequest req = new SendMessageRequest().withQueueUrl(queueUrl)
					.withMessageBody(tableDDL).withMessageAttributes(messageAttributes);
			try {
				SendMessageResult sendMsgRes = sqs.sendMessage(req);
				statusCode = sendMsgRes.getSdkHttpMetadata().getHttpStatusCode();
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Exception thrown while writing message to SQS. " + e.getLocalizedMessage());
			}
			if (statusCode == 200)
				System.out.printf("Table '%s' sent to SQS. \n", tableDDL);
		}
	}	/**
	 * This method creates MessageAttributeValue using a String value
	 *
	 * @param attributeValue
	 * @return
	 */
	public MessageAttributeValue createStringAttribute(final String attributeValue) {
		final MessageAttributeValue messageAttributeValue = new MessageAttributeValue().withDataType("String").withStringValue(attributeValue);
		return messageAttributeValue;
	}

}
