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

package com.datamelt.nifi.processors;

import java.io.IOException;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;

import com.datamelt.datagenerator.DataCreator;

/**
 * This Apache Nifi processor will generate data based on word lists, regular expressions or purely random.
 * 
 * 
 * 
 * @author uwe geercken - last update 2017-02-28
 */

@SideEffectFree
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"generate", "attributes", "data", "csv"})
@CapabilityDescription("Generates data based on word lists, regular expressions or purely random. The layout of the generated row(s) is defined in an external xml file")

public class GenerateData extends AbstractProcessor
{
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    
    DataCreator dataCreator = null;
    
    private static final String PROPERTY_CATEGORIES_PATH_NAME 		= "Categories folder";
    private static final String PROPERTY_ROWLAYOUT_PATH_NAME 		= "Rowlayout XML file folder and filename";
    private static final String PROPERTY_NUMBER_OF_OUTPUT_ROWS_NAME	= "Number of output rows";
    private static final String PROPERTY_MAXIMUM_YEAR_NAME			= "Maximum year for dates";
    private static final String PROPERTY_MINIMUM_YEAR_NAME			= "Minimum year for dates";
    
    private static final String PROPERTY_FILENAME	 				= "filename";
    private static final String RELATIONSHIP_SUCESS_NAME 			= "success";
    
    private static final String FIELD_SEPERATOR_PROPERTY_NAME 		= "Field separator";
    
    private static final String OUTPUT_FILENAME						= "datagenerator.csv";
    
    public static final PropertyDescriptor WORDLISTS_PATH = new PropertyDescriptor.Builder()
            .name(PROPERTY_CATEGORIES_PATH_NAME)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createDirectoryExistsValidator(false, false))
            .description("Specify the folder where the category files are located")
            .build();
    
    public static final PropertyDescriptor ROWLAYOUT_PATH = new PropertyDescriptor.Builder()
            .name(PROPERTY_ROWLAYOUT_PATH_NAME)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .description("Specify the folder and name of the rowlayout xml file")
            .build();
    
    public static final PropertyDescriptor NUMBER_OF_OUTPUT_ROWS = new PropertyDescriptor.Builder()
            .name(PROPERTY_NUMBER_OF_OUTPUT_ROWS_NAME)
            .description("The number of rows of CSV data to be generated for each flowfile")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor FIELD_SEPARATOR = new PropertyDescriptor.Builder()
            .name(FIELD_SEPERATOR_PROPERTY_NAME)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(";")
            .description("Specify the character to separate the generated fields of the output row from each other")
            .build();
    
    public static final PropertyDescriptor MAXIMUM_YEAR = new PropertyDescriptor.Builder()
            .name(PROPERTY_MAXIMUM_YEAR_NAME)
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("2099")
            .description("Specify the maximum generated year for date related fields")
            .build();
    
    public static final PropertyDescriptor MINIMUM_YEAR = new PropertyDescriptor.Builder()
            .name(PROPERTY_MINIMUM_YEAR_NAME)
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("2000")
            .description("Specify the maximum generated year for date related fields")
            .build();
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name(RELATIONSHIP_SUCESS_NAME)
            .description("The flow file content was successfully generated")
            .build();
    
    @Override
    public void init(final ProcessorInitializationContext context)
    {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(WORDLISTS_PATH);
        properties.add(ROWLAYOUT_PATH);
        properties.add(NUMBER_OF_OUTPUT_ROWS);
        properties.add(FIELD_SEPARATOR);
        properties.add(MAXIMUM_YEAR);
        properties.add(MINIMUM_YEAR);

        this.properties = Collections.unmodifiableList(properties);
        
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }
    
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws Exception
    {
    	getLogger().debug("creating DataCreator instance");
    	dataCreator = new DataCreator();
    	
    	getLogger().debug("setting categories files folder to: " + context.getProperty(WORDLISTS_PATH).getValue());
    	dataCreator.setCategoryFilesFolder(context.getProperty(WORDLISTS_PATH).getValue());
    	getLogger().debug("setting number of output lines to 1");
       	dataCreator.setNumberOfOutputLines(1);
       	getLogger().debug("setting field separator to: " + context.getProperty(FIELD_SEPARATOR).getValue());
       	dataCreator.setFieldSeparator(context.getProperty(FIELD_SEPARATOR).getValue());
       	getLogger().debug("setting maximum year for dates to: " + context.getProperty(MAXIMUM_YEAR).getValue());
       	dataCreator.setMaximumYear(context.getProperty(MAXIMUM_YEAR).asInteger());
       	getLogger().debug("setting minimum year for dates to: " + context.getProperty(MINIMUM_YEAR).getValue());
       	dataCreator.setMinimumYear(context.getProperty(MINIMUM_YEAR).asInteger());

    	getLogger().debug("parsing rowlayout file from: " + context.getProperty(ROWLAYOUT_PATH).getValue());
    	dataCreator.parseRowLayoutFile(context.getProperty(ROWLAYOUT_PATH).getValue());

    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException 
    {
    	// get a logger instance
    	final ComponentLog logger = getLogger();
    	
    	// map used to store the attribute name and its value from the content of the flowfile
        final Map<String, String> propertyMap = new HashMap<>();
        
    	// buffer to hold the content data
        StringBuffer content = new StringBuffer();

        try
        {
        	getLogger().debug("creating flow file");
        	FlowFile flowFile = session.create();
        	
        	getLogger().debug("generating rows: " + context.getProperty(NUMBER_OF_OUTPUT_ROWS).asInteger());
        	for(int i = 0; i < context.getProperty(NUMBER_OF_OUTPUT_ROWS).asInteger(); i++) 
	        {
	        	content.append(dataCreator.generateRow() );
	        	if(i<context.getProperty(NUMBER_OF_OUTPUT_ROWS).asInteger()-1)
	        	{
	        		content.append(System.lineSeparator());
	        	}
	        }
	        	
            if (content!=null && content.length() > 0) 
            {
            	flowFile = session.write(flowFile, new OutputStreamCallback() 
                {
                    @Override
                    public void process(final OutputStream out) throws IOException 
                    {
                    	getLogger().debug("writing generated data to flow file content");
                    	out.write(content.toString().getBytes());
                    }
                });
		    	// put the name of the ruleengine zip file in the list of properties
		        propertyMap.put(PROPERTY_FILENAME, OUTPUT_FILENAME);

            }
        	
            // put the map to the flow file
            flowFile = session.putAllAttributes(flowFile, propertyMap);
            
            session.getProvenanceReporter().create(flowFile);
            session.transfer(flowFile, SUCCESS);
        }
        catch(Exception ex)
        {
        	ex.printStackTrace();
            logger.error("error running the datagenerator",ex);
        }
    }
    
    @Override
    public Set<Relationship> getRelationships()
    {
        return relationships;
    }
    
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors()
    {
        return properties;
    }
}
