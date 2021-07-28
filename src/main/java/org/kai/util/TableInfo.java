// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package org.kai.util;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is a POJO class for Glue Database Table
 * It combines LargeTable and TableWithPartitions
 * When a Table is set, the partition information is also set.
 * - If partition size is at or below the threshold, just save it in the TableInfo to be serialized to SNS
 * - If partition size is above the threshold, persist partition data into the S3
 *
 */
public class TableInfo {


	private boolean largeTable;
	private int numberOfPartitions;
	private Table table;
	private String s3ObjectKey;
	private String s3BucketName;
	private String catalogId;
	private String region;
	private List<Partition> partitionList;
	final private int partitionThreshold = 5;

	public Table getTable() {
		return table;
	}

	public void setTable(Table table, String catalogId, String s3BucketName, String region) {
		this.table = table;
		this.catalogId = catalogId;
		this.region = region;
		setPartition(s3BucketName);
	}

	@Override
	public String toString() {
		return "TableInfo{" +
				"largeTable=" + largeTable +
				", numberOfPartitions=" + numberOfPartitions +
				", table=" + table +
				", s3ObjectKey='" + s3ObjectKey + '\'' +
				", s3BucketName='" + s3BucketName + '\'' +
				", catalogId='" + catalogId + '\'' +
				", region='" + region + '\'' +
				", partitionList=" + partitionList +
				", partitionThreshold=" + partitionThreshold +
				'}';
	}

	public String getCatalogId() {
		return catalogId;
	}
	public String getS3BucketName() {
		return s3BucketName;
	}
	public String getS3ObjectKey() {
		return s3ObjectKey;
	}
	public int getNumberOfPartitions() {
		return numberOfPartitions;
	}
	public int getPartitionThreshold() {
		return partitionThreshold;
	}

	public boolean isLargeTable() {
		return largeTable;
	}

	public List<Partition> getPartitionList() {
		if (!largeTable || partitionList != null) {  // small table, or large table but partition list already loaded
			System.out.println("Not Large Table.  Return Partition info from embedded SNS message");
		} else {  //  large table and partition list not yet loaded. So let's try to load it.
			System.out.println("Large Table.  Return Partition info from embedded S3 Object");
			String contentType = "";
			Gson gson = new Gson();
			S3Object fullObject = null;
			this.partitionList = new ArrayList<>();
			AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(this.region).build();
			try {
				fullObject = s3.getObject(new GetObjectRequest(this.s3BucketName, this.s3ObjectKey));
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Exception thrown while reading object from S3");
			}
			InputStream input = fullObject.getObjectContent();
			contentType = fullObject.getObjectMetadata().getContentType();
			System.out.println("CONTENT TYPE: " + contentType);
			BufferedReader reader = new BufferedReader(new InputStreamReader(input));
			String line = null;
			try {
				while ((line = reader.readLine()) != null) {
					Partition partition = gson.fromJson(line, Partition.class);
					partitionList.add(partition);
				}
			} catch (JsonSyntaxException | IOException e) {
				System.out.println("Exception occured while reading partition information from S3 object.");
				e.printStackTrace();
			}
			System.out.println("Number of partitions read from S3: " + partitionList.size());
		}
		return this.partitionList;
	}

	private void setPartition(String s3BucketName) {
		boolean objectCreated = false;

		GlueUtil glueUtil = new GlueUtil();

		// Client configuration
		ClientConfiguration cc = new ClientConfiguration();
		cc.setMaxErrorRetry(10);

		AWSGlue glue = AWSGlueClientBuilder.standard().withRegion(region).withClientConfiguration(cc).build();

		List<Partition> partitionList = glueUtil.getPartitions(glue, catalogId, table.getDatabaseName(), table.getName());
		this.numberOfPartitions = partitionList.size();

		if (this.numberOfPartitions <= partitionThreshold) {
			System.out.println("Not Large Table.  Embed partition info in SNS message.");
			this.largeTable = false;
			this.partitionList = partitionList;
		} else {
			System.out.println("Large Table.  Save partition info to S3 and set the S3 Bucket and Object key. Partition size: " + partitionList.size());

			// set Large Table, Bucket Name and Object Key
			this.largeTable = true;
			this.s3BucketName = s3BucketName;
			this.s3ObjectKey = initializeS3ObjectKey();

			S3Util s3Util = new S3Util();

			StringBuilder sb = new StringBuilder();
			AtomicInteger ai = new AtomicInteger();

			// Build partitions
			for (Partition p : partitionList) {
				Gson gson = new Gson();
				String partitionDDL = gson.toJson(p);
				sb.append(String.format("%s%n", partitionDDL));
				System.out.printf("Partition #: %d, schema: %s. \n", ai.incrementAndGet(), partitionDDL);
			}
			String partitionInfo =  sb.toString();
			System.out.println("Partition payload: " + partitionInfo);
			objectCreated = s3Util.createS3Object(region, this.s3BucketName, this.s3ObjectKey, partitionInfo);
			if (objectCreated) {
				System.out.println("Info: Success persisting partition info to S3 Bucket: " + this.s3BucketName + " Object Key: " + this.s3ObjectKey + " Partition Size: " + partitionList.size());
			} else {
				System.out.println("Error: Failed persisting partition info to S3 Bucket: " + this.s3BucketName + " Object Key: " + this.s3ObjectKey + " Partition Size: " + partitionList.size());
			}
		}
	}

	private String initializeS3ObjectKey() {
		// Create object key
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		StringBuilder date = new StringBuilder(simpleDateFormat.format(new Date()));
		return date.append("_").append(System.currentTimeMillis()).append("_")
				.append(catalogId).append("_").append(table.getDatabaseName())
				.append("_").append(table.getName()).append(".txt").toString();
	}

}
