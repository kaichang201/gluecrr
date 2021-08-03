// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package org.kai.lambda;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.regions.Regions;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.util.*;
import java.util.stream.Collectors;
import org.kai.util.GlueUtil;
import org.kai.util.SQSUtil;


/**
 * This class has AWS Lambda Handler method. Upon invocation, it fetches all the
 * databases form Glue Catalog, for each database, it takes the following
 * actions: 
 * 1. Convert Glue Database object to JSON String (This is Database DDL) 
 * 2. Publish the Database DDL to an SQS Topic
 *
 *
 */
public class PublishDatabaseFromDataCatalog implements RequestHandler<Object, String> {

	@Override
	public String handleRequest(Object input, Context context) {
		
		context.getLogger().log("Input: " + input);
		
		String region = Optional.ofNullable(System.getenv("region")).orElse(Regions.US_EAST_1.getName());
		String sourceGlueCatalogId = Optional.ofNullable(System.getenv("source_glue_catalog_id")).orElse("1234567890");
		String dbPrefixString = Optional.ofNullable(System.getenv("database_prefix_list")).orElse("");
		String list_separator = Optional.ofNullable(System.getenv("list_separator")).orElse(",");
		String separator = Optional.ofNullable(System.getenv("separator")).orElse("|");
		String sqsQueue4GlueDatabase = Optional.ofNullable(System.getenv("sqs_queue_url_glue_database")).orElse("");

		// Print environment variables
		printEnvVariables(sourceGlueCatalogId, sqsQueue4GlueDatabase, dbPrefixString, list_separator, separator);
		
		// Create Objects for Glue and SQS
		AWSGlue glue = AWSGlueClientBuilder.standard().withRegion(region).build();
		
		// Create Objects for Utility classes
		SQSUtil sqsUtil = new SQSUtil();
		GlueUtil glueUtil = new GlueUtil();
		ClientConfiguration cc = new ClientConfiguration();
		cc.setMaxErrorRetry(10);
		AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(region).withClientConfiguration(cc).build();
		
		// Get databases from Glue
		List<Database> dBList = glueUtil.getDatabases(glue, sourceGlueCatalogId);
		List<Database> publishDbList;

		// Tokenize the database prefix string to a List of database prefixes
		List<String> dbPrefixList = tokenizeDatabasePrefixString(dbPrefixString, region, list_separator, separator);

		// When database Prefix string is empty or not provided then, it imports all databases
		// else, it imports only the databases that has the same prefix
		if (dbPrefixList.size() == 0) {
			System.out.println("Publishing all");
			publishDbList = dBList;
		} else {
			// Identify required databases to export
			publishDbList = getRequiredDatabases(dBList, dbPrefixList);
			System.out.println("Publishing matched: " + publishDbList);
		}
		System.out.printf(
				"Database export statistics: number of databases exist = %d, number of databases matching prefix = %d. \n",
				dBList.size(), publishDbList.size());

		if (publishDbList.size() == 0 ) {
			System.out.println("Not exporting any DB.  DBList size: 0 ");
		} else {
			sqsUtil.publishDatabasesToSQS(sqs, sqsQueue4GlueDatabase, publishDbList, sourceGlueCatalogId);
		}
		return "Lambda function to get a list of Databases completed successfully!";
	}
	
	/**
	 * This method prints environment variables
	 * @param sourceGlueCatalogId
	 * @param sqsQueue4GlueDatabase
 	 * @param dbPrefixString
  	 * @param list_separator
 	 * @param separator
	 */
	public static void printEnvVariables(String sourceGlueCatalogId, String sqsQueue4GlueDatabase,
										 String dbPrefixString, String list_separator, String separator) {
		System.out.println("SQS Queue URL: " + sqsQueue4GlueDatabase);
		System.out.println("Source Catalog Id: " + sourceGlueCatalogId);
		System.out.println("Database Prefix String: " + dbPrefixString);
		System.out.println("List Separator: " + list_separator);
		System.out.println("Prefix Separator: " + separator);
	}
	
	/**
	 * Tokenize the Data Prefix String to a List of Prefixes
	 * @param dbPrefixString
 	 * @param region
 	 * @param list_separator
	 * @param separator
	 * @return
	 */
	public static List<String> tokenizeDatabasePrefixString(String dbPrefixString, String region, String list_separator, String separator) {
		List<String> dbPrefixesList = new ArrayList<>();
		List<String> regionList = Collections.list(new StringTokenizer(dbPrefixString, list_separator)).stream()
	      .map(token -> (String) token)
	      .collect(Collectors.toList());
		System.out.println("Number of regions prefixes: " + regionList.size());
		for (String regionL: regionList) {
			List<String> parseList = Collections.list(new StringTokenizer(regionL, separator)).stream()
					.map(token -> (String) token)
					.collect(Collectors.toList());
			if (parseList.size() > 0 && region.equals(parseList.get(0))) {
				System.out.println("Info: Found matching region." + parseList);
				for (int i=1; i<parseList.size(); i++)
					dbPrefixesList.add(parseList.get(i));
			}
		}
		System.out.println("Region: " + region + " primary databases are: " + dbPrefixesList);
		return dbPrefixesList;
	}

	/**
	 * 
	 * @param dBList
	 * @param requiredDBPrefixList
	 * @return
	 */
	public static List<Database> getRequiredDatabases(List<Database> dBList, List<String> requiredDBPrefixList){
		
		List<Database> dBsToExportList = new ArrayList<Database>();
		for(Database database : dBList) {
			for(String dbPrefix : requiredDBPrefixList) {
				if(database.getName().toLowerCase().startsWith(dbPrefix)) {
					dBsToExportList.add(database);
					break;
				}
			}
		}
		System.out.printf("Number of databases in Glue Catalog: %d, number of databases to be exported: %d \n", dBList.size(), dBsToExportList.size());
		return dBsToExportList;
	}
}