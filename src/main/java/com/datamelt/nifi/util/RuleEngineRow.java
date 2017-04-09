/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datamelt.nifi.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper class for the ExecuteRuleEngine processor.
 * 
 * An instance of the object stored a row of data from a CSV row and a map
 * with flow file properties.
 * 
 * The values from the map (passed by reference) are copied to a new map instance
 * because the referenced map changes in the processor. 
 * 
 * @author uwe geercken, - last update 2017-04-09
 *
 */
public class RuleEngineRow
{
	// a row of csv data
	private String row;
	// map of properties
	private HashMap<String, String> map = new HashMap<String, String>();
	
	/**
	 * Constructor for 
	 * 
	 * @param row		a row of CSV data
	 * @param map		a map with properties
	 */
	public RuleEngineRow(String row, Map<String, String> map)
	{
		this.row = row;
		this.map.putAll(map);
	}

	/**
	 * a row of CSV data
	 * 
	 * @return		the row that was passed to this instance
	 */
	public String getRow()
	{
		return row;
	}

	/**
	 * a map of properties copied from the flow file properties
	 * 
	 * @return		 map of properties
	 */
	public HashMap<String, String> getMap()
	{
		return map;
	}
}
