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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipFile;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.datamelt.rules.core.RuleExecutionResult;
import com.datamelt.rules.engine.BusinessRulesEngine;
import com.datamelt.util.RowField;
import com.datamelt.util.RowFieldCollection;
import com.datamelt.util.Splitter;

/**
 * This Apache Nifi processor will allow to run the business rules engine JaRE against
 * an incoming flow file. The results of the ruleengine will be stored in the flow file
 * attributes and may then be used for further decisions/routing in the Nifi flow.
 * <p>
 * The ruleengine uses a project zip file, which was created in the Business Rules Maintenance Tool.
 * This is a web application to construct and orchestrate the business rules logic. The
 * tool allows to export the logic of a project into a single zip file.
 * <p>
 * The content of the flow file is expected to be one row of comma separated data. The row
 * is split into its individual fields using the given field separator.
 * <p>

 * @author uwe geercken - last update 2017-03-06
 */

@SideEffectFree
@Tags({"CSV", "ruleengine", "filter", "decision", "logic", "business rules"})
@CapabilityDescription("Uses the Business Rules Engine JaRE to execute a ruleengine file containing business logic against the flow file content."
        + " The flowfile content is expected to be a single row of data in CSV format. This row of data is split into it's individual fields"
		+ " and then the business logic from the project zip file is applied to the fields. If actions are defined these may update the flow file content."
        + " The ruleengine file (zip format) is created by exporting a project from the Business Rules Maintenance Tool - a web application to construct and orchestrate business logic."
		+ " Because the business logic is separated from the Nifi flow and processors, when the business logic changes, the Nifi flow does not have to be changed. "
        + " Instead the business logic is updated in the Business Rules maintenance tool and a new project zip file is created."
		+ " If a header row is present, the header row is split into it's individual fields and these are passed to the ruleengine. This allows the rules to reference the names of the fields instead of their index number in the row."
        + " A header row can only be a single row, needs to be present in each flow file and has to use the same separator as the data."
		+ " If more than one row is found (or two, when a header row is present), then the rest of the content will be ignored by the ruleengine and removed from the flow file content."
		)
@WritesAttributes({ @WritesAttribute(attribute = "ruleengine.zipfile", description = "The name of the ruleengine project zip file that was used"),
@WritesAttribute(attribute = "ruleengine.rulegroupsCount", description = "The number of rulegroups in the ruleengine project zip file"),
@WritesAttribute(attribute = "ruleengine.rulegroupsPassed", description = "The number of rulegroups that passed the business logic after running the ruleengine"),
@WritesAttribute(attribute = "ruleengine.rulegroupsFailed", description = "The number of rulegroups that failed the business logic after running the ruleengine"),
@WritesAttribute(attribute = "ruleengine.rulegroupsSkipped", description = "The number of rulegroups that where skipped - because of rulegroup dependencies - when running the ruleengine"),
@WritesAttribute(attribute = "ruleengine.ruleCount", description = "The number of rules in the ruleengine project zip file"),
@WritesAttribute(attribute = "ruleengine.rulesPassed", description = "The number of rules that passed the business logic after running the ruleengine"),
@WritesAttribute(attribute = "ruleengine.rulesFailed", description = "The number of rules that failed the business logic after running the ruleengine"),
@WritesAttribute(attribute = "ruleengine.actionsCount", description = "The number of actions in the ruleengine project zip file"),
@WritesAttribute(attribute = "ruleengine.dataModified", description = "Indicator if the flow file content was modified based on one or multiple actions in the ruleengine project zip file")})
@Stateful(scopes = {Scope.LOCAL}, description = "The name and the last modified date of the ruleengine project zip file is captured when the processor is scheduled. This creates a reference to the filename and last modified date in case the processor runs for a longer period or in case the file meanwhile has changed.")
public class RuleEngine extends AbstractProcessor 
{
    // list of properties
	private List<PropertyDescriptor> properties;
	// set of relationships
    private Set<Relationship> relationships;

    private String separator="";
    
    // the business rules engine to execute business logic against data
    private static BusinessRulesEngine ruleEngine 								= null;
    
    //these fields from the results of the ruleengine will be added to the flow file attributes
    private static final String PROPERTY_RULEENGINE_ZIPFILE_NAME 				= "ruleengine.zipfile";
    private static final String PROPERTY_RULEENGINE_RULEGROUPS_COUNT			= "ruleengine.rulegroupsCount";
    private static final String PROPERTY_RULEENGINE_RULEGROUPS_PASSED 			= "ruleengine.rulegroupsPassed";
    private static final String PROPERTY_RULEENGINE_RULEGROUPS_FAILED 			= "ruleengine.rulegroupsFailed";
    private static final String PROPERTY_RULEENGINE_RULEGROUPS_SKIPPED 			= "ruleengine.rulegroupsSkipped";
    private static final String PROPERTY_RULEENGINE_RULES_COUNT 				= "ruleengine.rulesCount";
    private static final String PROPERTY_RULEENGINE_RULES_PASSED 				= "ruleengine.rulesPassed";
    private static final String PROPERTY_RULEENGINE_RULES_FAILED	 			= "ruleengine.rulesFailed";
    private static final String PROPERTY_RULEENGINE_ACTIONS_COUNT 				= "ruleengine.actionsCount";
    private static final String PROPERTY_RULEENGINE_CONTENT_MODIFIED 			= "ruleengine.contentModified";
    
    // names/labels of the processor attibutes
    private static final String RULEENGINE_ZIPFILE_PROPERTY_NAME 				= "Ruleengine Project Zip File";
    private static final String FIELD_SEPERATOR_PROPERTY_NAME 					= "Field Separator";
    private static final String HEADER_PRESENT_PROPERTY_NAME 					= "Header present";
    private static final String FIELD_NAMES_PROPERTY_NAME 						= "Field Names";
    private static final String RULEENGINE_OUTPUT_DETAILS 						= "Output detailed results";
    
    // relationships
    private static final String RELATIONSHIP_SUCESS_NAME 						= "success";
    private static final String RELATIONSHIP_DETAILED_OUTPUT_NAME				= "detailed output";

    // separator used to split up the defined field names
    private static final String FIELD_NAMES_SEPARATOR 							= ",";
    
    private static final String FIELD_SEPERATOR_POSSIBLE_VALUE_COMMA			= "Comma";
    private static final String FIELD_SEPERATOR_POSSIBLE_VALUE_SEMICOLON		= "Semicolon";
    private static final String FIELD_SEPERATOR_POSSIBLE_VALUE_TAB				= "Tab";
    
    private static final String SEPERATOR_COMMA									= ",";
    private static final String SEPERATOR_SEMICOLON								= ";";
    private static final String SEPERATOR_TAB									= "\t";
    
    private static final String STATE_MANAGER_FILENAME							= "ruleengine.zipfile.latest";
    private static final String STATE_MANAGER_FILENAME_LASTMODIFIED				= "ruleengine.zipfile.lastModified";
    
    // properties of the processor
    public static final PropertyDescriptor ATTRIBUTE_RULEENGINE_ZIPFILE = new PropertyDescriptor.Builder()
            .name(RULEENGINE_ZIPFILE_PROPERTY_NAME)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .description("Specify the path and filename of the ruleengine project file to use. Build this file with the Business Rules Maintenance Tool - a web application for constructing and orchestrating business logic.")
            .build();
    
    public static final PropertyDescriptor ATTRIBUTE_HEADER_PRESENT = new PropertyDescriptor.Builder()
            .name(HEADER_PRESENT_PROPERTY_NAME)
            .required(true)
            .allowableValues("true","false")
            .defaultValue("false")
            .description("Specify if each flow file content contains a (single line) header row")
            .build();

    public static final PropertyDescriptor ATTRIBUTE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name(FIELD_NAMES_PROPERTY_NAME)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specify the names of the individual fields of the row, in their correct order and separated by a comma. Ignored if attribute [" + HEADER_PRESENT_PROPERTY_NAME + "] is set to true.")
            .build();
    
    public static final PropertyDescriptor ATTRIBUTE_FIELD_SEPARATOR = new PropertyDescriptor.Builder()
            .name(FIELD_SEPERATOR_PROPERTY_NAME)
            .required(true)
            .allowableValues(FIELD_SEPERATOR_POSSIBLE_VALUE_COMMA,FIELD_SEPERATOR_POSSIBLE_VALUE_SEMICOLON, FIELD_SEPERATOR_POSSIBLE_VALUE_TAB)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specify the field separator to be used to split the incomming flow file content. The content must be a single row of CSV data. This separator is also used to split the fields of the header row.")
            .build();
    
    public static final PropertyDescriptor ATTRIBUTE_OUTPUT_DETAILED_RESULTS = new PropertyDescriptor.Builder()
            .name(RULEENGINE_OUTPUT_DETAILS)
            .required(true)
            .allowableValues("true","false")
            .defaultValue("false")
            .description("Specify if the detailed results should be output. This creates flow files containing one row of data per rule (!). E.g. if you have one row of data and 10 rules, the flowfile contains 10 rows with the detailed results for each rule.")
            .build();
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name(RELATIONSHIP_SUCESS_NAME)
            .description("The ruleengine successfully executed the business rules against the flow file content.")
            .build();

    public static final Relationship DETAILED_RESULTS = new Relationship.Builder()
            .name(RELATIONSHIP_DETAILED_OUTPUT_NAME)
            .description("The ruleengine successfully executed the business rules against the flow file content.")
            .build();

    @Override
    public void init(final ProcessorInitializationContext context) 
    {
    	// add properties to list
    	List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ATTRIBUTE_RULEENGINE_ZIPFILE);
        properties.add(ATTRIBUTE_HEADER_PRESENT);
        properties.add(ATTRIBUTE_FIELD_NAMES);
        properties.add(ATTRIBUTE_FIELD_SEPARATOR);
        properties.add(ATTRIBUTE_OUTPUT_DETAILED_RESULTS);
        this.properties = Collections.unmodifiableList(properties);

        // add relationships to set
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(DETAILED_RESULTS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }
    
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws Exception
    {
        // faced problems when having \t as an attribute and could not find a solution
    	// so using allowable values instead and translating them here
    	if(context.getProperty(ATTRIBUTE_FIELD_SEPARATOR).getValue().equals(FIELD_SEPERATOR_POSSIBLE_VALUE_COMMA))
        {
        	separator = SEPERATOR_COMMA;
        }
        else if(context.getProperty(ATTRIBUTE_FIELD_SEPARATOR).getValue().equals(FIELD_SEPERATOR_POSSIBLE_VALUE_SEMICOLON))
        {
        	separator = SEPERATOR_SEMICOLON;
        }
        else if(context.getProperty(ATTRIBUTE_FIELD_SEPARATOR).getValue().equals(FIELD_SEPERATOR_POSSIBLE_VALUE_TAB))
        {
        	separator = SEPERATOR_TAB;
        }
    	
    	// get the zip file, containing the business rules
        File file = new File(context.getProperty(ATTRIBUTE_RULEENGINE_ZIPFILE).getValue());
        if(file.exists() && file.isFile())
        {
        	// get a state manager instance
        	StateManager stateManager = context.getStateManager();
        	//StateMap state = stateManager.getState(Scope.LOCAL);
        	
        	// put filename and lastmodified date into hashmap
        	// so we have a reference which file is currently used with the processor
        	HashMap<String,String> fileMap = new HashMap<String,String>();
        	fileMap.put(STATE_MANAGER_FILENAME, file.getName());
        	fileMap.put(STATE_MANAGER_FILENAME_LASTMODIFIED, new Date(file.lastModified()).toString());
        	
        	// put the hashmap to statemanager
        	context.getStateManager().setState(fileMap, Scope.LOCAL);
        	
        	// get the last modified date of the file
        	Long fileLastModified = file.lastModified();
        	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
        	String fileLastModifiedDateAsString = sdf.format(new Date(fileLastModified)); 
        	
        	// the file should be a zip file containing the business logic
        	ZipFile ruleEngineProjectFile = new ZipFile(file);
        	getLogger().debug("got zip file: " + context.getProperty(ATTRIBUTE_RULEENGINE_ZIPFILE).getValue());
        	
        	try
        	{
            	// create ruleengine instance with the zip file
                ruleEngine = new BusinessRulesEngine(ruleEngineProjectFile);
                
                // output basic info about the ruleengine initialization
                getLogger().info("initialized business rule engine version: " + BusinessRulesEngine.getVersion() + " using " + context.getProperty(ATTRIBUTE_RULEENGINE_ZIPFILE).getValue() + ", last modified: [" + fileLastModifiedDateAsString + "]");
                
                getLogger().debug("field separator to split row into fields: " + context.getProperty(ATTRIBUTE_FIELD_SEPARATOR).getValue());
            
                // we display the number of rulegroups contained in the zip file
                getLogger().debug("number of rulegroups in project zip file: " + ruleEngine.getNumberOfGroups());

                // we do not want to keep the detailed results if the user does not need them
                if(!context.getProperty(ATTRIBUTE_OUTPUT_DETAILED_RESULTS).getValue().equals("true"))
                {
                	ruleEngine.setPreserveRuleExcecutionResults(false);
                }
        		
        	}
        	catch(Exception ex)
        	{
        		// something went wrong
        		// maybe the project zip file (contains xml files) could not be parsed
        		ex.printStackTrace();
        	}
        }
        else
        {
        	throw new Exception("the ruleengine project zip file was not found or is not a file");
        }
    }
    
    @OnUnscheduled
    public void onUnScheduled(final ProcessContext context) throws Exception
    {
        // reset the ruleengine instance
    	ruleEngine = null;
        getLogger().debug("processor unscheduled - set ruleengine instance to null");
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException 
    {
    	// map used to store the attribute name and its value from the content of the flow file
        final Map<String, String> propertyMap = new HashMap<>();
    	
        // get a logger instance
    	final ComponentLog logger = getLogger();
    	
    	final AtomicReference<Boolean> collectionUpdated = new AtomicReference<>();
    	collectionUpdated.set(false);
    	
    	final AtomicReference<String> contentUpdated = new AtomicReference<>();
    	
    	// get the flow file
    	FlowFile flowFile = session.get();
        if (flowFile == null) 
        {
            return;
        }

        // read flow file into input stream
        session.read(flowFile, new InputStreamCallback() 
        {
            public void process(InputStream in) throws IOException 
            {
                try 
                {
                    // get the flow file content
                    String originalContent = IOUtils.toString(in, "UTF-8");
                    String headerRow = null;
                    String row = null;
                    String[] originalContentRows = originalContent.split(System.lineSeparator());
                    // check if header row is present
                    if(context.getProperty(ATTRIBUTE_HEADER_PRESENT).getValue().equals("true"))
                    {
                    	logger.debug("configuration indicates 1 header row is present");
                    	
                    	if(originalContentRows!=null && originalContentRows.length==2)
                    	{
                    		logger.debug("found 2 rows in flow file content - assuming 1 header row and 1 data row");
                    		// first row is the header row
                    		headerRow = originalContentRows[0];
                    		// second row is the data
                    		row = originalContentRows[1];
                    	}
                    	else if(originalContentRows!=null && originalContentRows.length==1)
                    	{
                    		logger.debug("found 1 row in flow file content - assuming 1 header row");
                    		// first row is the header row but no further rows found (no data)
                    		headerRow = originalContentRows[0];
                    	}
                    	else if(originalContentRows!=null && originalContentRows.length==0)
                    	{
                    		// flow file has no rows - is empty
                    		logger.debug("found 0 rows in flow file content");
                    	}
                    	else
                    	{
                    		if(originalContentRows!=null)
                    		{
                    			logger.warn("found more than 2 rows in flow file content - assuming header row and data row and ignoring additional rows");
                    			// first row is the header row
                    			headerRow = originalContentRows[0];
                    			// second row is the data
                    			row = originalContentRows[1];
                    		}
                    	}
                    }
                    // if no header row is present but field names are defined
                    else if(context.getProperty(ATTRIBUTE_FIELD_NAMES)!=null && !context.getProperty(ATTRIBUTE_FIELD_NAMES).getValue().equals(""))
                    {
                    	logger.debug("configuration indicates no header row is present but field names are defined");
                    	// get the defined field names
                    	headerRow = context.getProperty(ATTRIBUTE_FIELD_NAMES).getValue();
                    	if(originalContentRows!=null && originalContentRows.length==1)
                    	{
                    		// first row is the data
                    		row = originalContentRows[0];
                    	}
                    	else if(originalContentRows!=null && originalContentRows.length==0)
                    	{
                    		// flow file has no rows - is empty
                    		logger.debug("found 0 rows in flow file content");
                    	}
                    	else
                    	{
                    		if(originalContentRows!=null)
                    		{
                    			logger.warn("found more than 1 row in flow file content - assuming data row and ignoring additional rows");	
                    			// first row is the data
                    			row = originalContentRows[0];
                    		}
                    	}
                    }
                    // if no header row is present and no field names are defined
                    else
                    {
                    	logger.debug("configuration indicates no header row is present and no field names are defined");
                    	
                    	if(originalContentRows!=null && originalContentRows.length==1)
                    	{
                    		// first row is the data
                    		row = originalContentRows[0];
                    	}
                    	else if(originalContentRows!=null && originalContentRows.length==0)
                    	{
                    		// flow file has no rows - is empty
                    		logger.debug("found 0 rows in flow file content");
                    	}
                    	else
                    	{
                    		if(originalContentRows!=null)
                    		{
                    			logger.warn("found more than 1 row in flow file content - assuming data row and ignoring additional rows");	
                    			// first row is the data
                    			row = originalContentRows[0];
                    		}
                    	}
                    }
                    // check that we have data
                    if (row != null && !row.trim().equals("")) {
                        
                        // use the Splitter class to split the incoming row into fields
                    	// pass info which separator is used
                		Splitter splitter = new Splitter(Splitter.TYPE_COMMA_SEPERATED,separator);
                		logger.debug("created Splitter object");

                		RowFieldCollection collection = null;
                		
                		// convert the fields of the CSV file into a collection
                		// the given field separator is used to split the incoming data into fields
                		// if we have a header row, use the field names for the ruleengine
                		if(headerRow!=null && !headerRow.trim().equals(""))
                		{
                			String[] headerFields;
                			// split header row into fields
                			if(context.getProperty(ATTRIBUTE_HEADER_PRESENT).getValue().equals("true"))
                			{
                				headerFields = headerRow.split(separator);
                			}
                			// split defined field names
                			else
                			{
                				headerFields = headerRow.split(FIELD_NAMES_SEPARATOR);
                			}
                			// create the collection with header fields
                			collection = new RowFieldCollection(headerFields,splitter.getFields(row));
                			logger.debug("splitted header row into fields: " + headerFields.length + " number of fields");
                		}
                		else
                		{
                			// no header row - create collection without header fields
                			// this means that the fields can only be called (from the ruleengine) by their number
                			collection = new RowFieldCollection(splitter.getFields(row));
                			logger.debug("created RowFieldCollection object: " + collection.getNumberOfFields() + " number of fields");
                		}

        		        logger.debug("running business ruleengine...");
        		        
        		        // run the ruleengine with the given data from the flow file
        		        ruleEngine.run("flowfile",collection);
        		        
        		        logger.debug("number of rulegroups: " + ruleEngine.getNumberOfGroups());
        		        logger.debug("number of rulegroups passed: " + ruleEngine.getNumberOfGroupsPassed());
        		        logger.debug("number of rulegroups failed: " + ruleEngine.getNumberOfGroupsFailed());
        		        logger.debug("number of rulegroups skipped: " + ruleEngine.getNumberOfGroupsSkipped());
        		        logger.debug("number of rules: " + ruleEngine.getNumberOfRules());
        		        logger.debug("number of rules passed: " + ruleEngine.getNumberOfRulesPassed());
        		        logger.debug("number of rules failed: " + ruleEngine.getNumberOfRulesFailed());
        		        logger.debug("number of actions: " + ruleEngine.getNumberOfActions());
        		    	
        		    	// put the name of the ruleengine zip file in the list of properties
        		        propertyMap.put(PROPERTY_RULEENGINE_ZIPFILE_NAME, context.getProperty(ATTRIBUTE_RULEENGINE_ZIPFILE).getValue() );
        		        
        		        // put the total number of  rulegroups in the property map
        		        propertyMap.put(PROPERTY_RULEENGINE_RULEGROUPS_COUNT, ""+ ruleEngine.getNumberOfGroups());
        		        
        		        // put the number of passed rulegroups in the property map
        		        propertyMap.put(PROPERTY_RULEENGINE_RULEGROUPS_PASSED, ""+ ruleEngine.getNumberOfGroupsPassed());
        		        
        		        // put the number of failed rulegroups in the property map
        		        propertyMap.put(PROPERTY_RULEENGINE_RULEGROUPS_FAILED, ""+ ruleEngine.getNumberOfGroupsFailed());
        		        
        		        // put the number of skipped rulegroups in the property map
        		        propertyMap.put(PROPERTY_RULEENGINE_RULEGROUPS_SKIPPED, ""+ ruleEngine.getNumberOfGroupsSkipped());
        		        
        		        // put the total number of rules in the property map
        		        propertyMap.put(PROPERTY_RULEENGINE_RULES_COUNT, ""+ ruleEngine.getNumberOfRules());
        		        
        		        // put the number of passed rules in the property map
        		        propertyMap.put(PROPERTY_RULEENGINE_RULES_PASSED, ""+ ruleEngine.getNumberOfRulesPassed());
        		        
        		        // put the number of failed rules in the property map
        		        propertyMap.put(PROPERTY_RULEENGINE_RULES_FAILED, ""+ ruleEngine.getNumberOfRulesFailed());
        		        
        		        // put the total number of actions in the property map
        		        propertyMap.put(PROPERTY_RULEENGINE_ACTIONS_COUNT, ""+ ruleEngine.getNumberOfActions());
        		        
        		        // buffer to hold the row data
        		        StringBuffer content = new StringBuffer();
        		        
        		        // append the header row if present
        		        if(context.getProperty(ATTRIBUTE_HEADER_PRESENT).getValue().equals("true") && headerRow!=null && !headerRow.trim().equals(""))
        		        {
        		        	content.append(headerRow);
        		        	// if the header row does not have a line seperator at the end then add it
        		        	if(!headerRow.endsWith(System.lineSeparator()))
        		        	{
        		        		content.append(System.lineSeparator());
        		        	}
        		        }
        		        
        		        // process only if the collection of fields was changed by
        		        // a ruleengine action. this means the data was updated so
        		        // we have to re-write the flow file content
       		        	if(collection.isCollectionUpdated())
        		        {
       		        		collectionUpdated.set(true);
       		        		logger.debug("data was modified by an ruleengine action - reconstructing content");
       		        		// loop through the collection and construct the output row
       		        		for(int i=0;i<collection.getFields().size();i++)
    			            {
    			           		RowField rf = collection.getField(i);
			           			content.append(rf.getValue());
			           			if(i<collection.getFields().size()-1)
			           			{
			           				//content.append(context.getProperty(ATTRIBUTE_FIELD_SEPARATOR).getValue());
			           				content.append(separator);
			           			}
    			            }
    			        	
       		        		// store the result in an atomic reference
    			        	contentUpdated.set(content.toString());
       		        	}
       		        	else
       		        	{
       		        		// no change was made by the ruleengine
       		        		contentUpdated.set(originalContent);
       		        	}

                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    logger.error("error running the business ruleengine",ex);
                }
            }
        });

        // if the data was updated (by an action)
        if(collectionUpdated.get()==true)
        {
	        // put an indicator that the data was modified by the ruleengine
	        propertyMap.put(PROPERTY_RULEENGINE_CONTENT_MODIFIED, "true");

	        // write data to output stream
        	flowFile = session.write(flowFile, new OutputStreamCallback() 
        	{
                   @Override
                   public void process(final OutputStream out) throws IOException 
                   {
                	   final byte[] data = contentUpdated.get().getBytes();
                	   out.write(data);
                   }
               });
        }
        else
        {
	        // put an indicator that the data was NOT modified by the ruleengine
	        propertyMap.put(PROPERTY_RULEENGINE_CONTENT_MODIFIED, "false");
        }
    	
        // put the map to the flow file
        flowFile = session.putAllAttributes(flowFile, propertyMap);
        
        // if the user wants detailed results, then we clone the flow file, add the ruleengine message for 
        // each rule to the properties and transmit the flow file to the detailed relationship
        if(context.getProperty(ATTRIBUTE_OUTPUT_DETAILED_RESULTS).getValue().equals("true"))
        {
	        // get the collection of results from the Rule Engine instance
	 		ArrayList<RuleExecutionResult> results = ruleEngine.getRuleExecutionCollection().getResults();

			// create a clone of the original flow file
			FlowFile detailedFlowFile = session.clone(flowFile);

			// append a separator and the ruleengine message
			detailedFlowFile = session.write(detailedFlowFile, new OutputStreamCallback() 
        	{
                   @Override
                   public void process(final OutputStream out) throws IOException 
                   {
                	   	StringBuffer detailedFlowFileContent = new StringBuffer();

	               	   	// loop over the ruleengine results
	   					for(int i=0;i<results.size();i++)
	   					{
	   						// get the result 
	   					    RuleExecutionResult result = results.get(i);
	   					    detailedFlowFileContent.append(contentUpdated.get());
	   					    detailedFlowFileContent.append(separator);
	   					    detailedFlowFileContent.append(result.getMessage());
	   					    
	   					    // add line separator except last line
	   					    if(i<results.size()-1)
	   					    {
	   					    	detailedFlowFileContent.append(System.lineSeparator());
	   					    }
	   					}
	               	   
	   					// write data to content
	   					final byte[] data = detailedFlowFileContent.toString().getBytes();
	   					out.write(data);
                   }
            });
			
			// transfer the flow file
	        session.transfer(detailedFlowFile, DETAILED_RESULTS);
	        
			// for provenance reporting
			session.getProvenanceReporter().route(detailedFlowFile, DETAILED_RESULTS);

        }

        // for provenance reporting
        session.getProvenanceReporter().modifyAttributes(flowFile);
        
        // transfer the flow file
        session.transfer(flowFile, SUCCESS);

        // clear the collections of ruleengine results
    	ruleEngine.getRuleExecutionCollection().clear();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
}
