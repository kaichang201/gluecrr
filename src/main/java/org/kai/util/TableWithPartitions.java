// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package org.kai.util;

import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.Table;

import java.util.List;

public class TableWithPartitions {

	private Table table;
	private List<Partition> partitionList;
	
	public Table getTable() {
		return table;
	}
	public void setTable(Table table) {
		this.table = table;
	}
	public List<Partition> getPartitionList() {
		return partitionList;
	}
	public void setPartitionList(List<Partition> partitionList) {
		this.partitionList = partitionList;
	}
	
	
	
}
