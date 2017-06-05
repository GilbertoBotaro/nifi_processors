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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipFile;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
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

import com.datamelt.nifi.util.RuleEngineRow;
import com.datamelt.rules.core.RuleExecutionResult;
import com.datamelt.rules.core.RuleGroup;
import com.datamelt.rules.core.RuleSubGroup;
import com.datamelt.rules.core.XmlRule;
import com.datamelt.rules.engine.BusinessRulesEngine;
import com.datamelt.util.HeaderRow;
import com.datamelt.util.RowField;
import com.datamelt.util.RowFieldCollection;
import com.datamelt.util.Splitter;

/**
 * This Apache Nifi processor will allow to run the business rules engine JaRE against
 * an incoming flow file. The results of the ruleengine will be stored in the flow file
 * attributes and may then be used for further decisions/routing in the Nifi flow.
 * <p>
 * The ruleengine uses a project zip file (containing the business logic), which is created using the Business Rules Maintenance Tool.
 * This is a web application to construct and orchestrate the business rules logic. The tool allows to export the logic of a project into a single zip file.
 * <p>
 * The content of the flow file is expected to be one or multiple rows of comma separated data (CSV). The rows are split into its individual fields using the given field separator.
 * <p>

 * @author uwe geercken - last update 2017-04-09
 */

@SideEffectFree
@Tags({"CSV", "ruleengine", "filter", "decision", "logic", "business rules"})
@CapabilityDescription("Uses the Business Rules Engine JaRE to execute a ruleengine file containing business logic against the flow file content."
        + " The flowfile content is expected to be a single or multiple rows of data in CSV format. Each row of data is split into it's individual fields"
		+ " and then the business logic from the rules in the project zip file is applied to the fields. If actions are defined these may update the flow file content."
        + " The ruleengine file (zip format) is created by exporting a project from the Business Rules Maintenance Tool - a web application to construct and orchestrate business logic."
		+ " Because the business logic is separated from the Nifi flow and processors, when the business logic changes, the Nifi flow does not have to be changed. "
        + " Instead the business logic is updated in the Business Rules maintenance tool and a new project zip file is created."
		+ " If a header row is present, the header row is split into it's individual fields and these are passed to the ruleengine. This allows the rules to reference the names of the fields instead of their index number in the row."
        + " A header row can only be a single row, needs to be present in each flow file and has to use the same separator as the data."
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
public class ExecuteRuleEngine extends AbstractProcessor 
{
    // list of properties
	private List<PropertyDescriptor> properties;
	// set of relationships
    private Set<Relationship> relationships;
    
    // separator of the csv data
    private String separator;

    // HeaderRow instance containing the given field names
    private static HeaderRow headerFromFieldNames;
    
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
    
    // names/labels of the processor attributes
    private static final String RULEENGINE_ZIPFILE_PROPERTY_NAME 				= "Ruleengine Project Zip File";
    private static final String FIELD_SEPERATOR_PROPERTY_NAME 					= "Field Separator";
    private static final String HEADER_PRESENT_PROPERTY_NAME 					= "Header row present";
    private static final String FIELD_NAMES_PROPERTY_NAME 						= "Field Names";
    private static final String RULEENGINE_OUTPUT_DETAILS 						= "Output detailed results";
    private static final String RULEENGINE_OUTPUT_DETAILS_TYPE					= "Output detailed results type";
    private static final String BATCH_SIZE_NAME									= "Rows batch size";
    
    // relationships
    private static final String RELATIONSHIP_ORIGINAL_NAME 						= "original";
    private static final String RELATIONSHIP_SUCESS_NAME 						= "success";
    private static final String RELATIONSHIP_DETAILED_OUTPUT_NAME				= "detailed output";
    private static final String RELATIONSHIP_FAILURE_NAME						= "failure";

    // separator used to split up the defined field names
    private static final String FIELD_NAMES_SEPARATOR 							= ",";
    
    // possible delimiter types
    private static final String FIELD_SEPARATOR_POSSIBLE_VALUE_COMMA			= "Comma";
    private static final String FIELD_SEPARATOR_POSSIBLE_VALUE_SEMICOLON		= "Semicolon";
    private static final String FIELD_SEPARATOR_POSSIBLE_VALUE_TAB				= "Tab";
    
    // output types for ruleengine details
    private static final String OUTPUT_TYPE_ALL_GROUPS_ALL_RULES				= "all groups - all rules";
    private static final String OUTPUT_TYPE_FAILED_GROUPS_FAILED_RULES			= "failed groups - failed rules only ";
    private static final String OUTPUT_TYPE_FAILED_GROUPS_PASSED_RULES			= "failed groups - passed rules only ";
    private static final String OUTPUT_TYPE_FAILED_GROUPS_ALL_RULES				= "failed groups - all rules";
    private static final String OUTPUT_TYPE_PASSED_GROUPS_FAILED_RULES			= "passed groups - failed rules only";
    private static final String OUTPUT_TYPE_PASSED_GROUPS_PASSED_RULES			= "passed groups - passed rules only";
    private static final String OUTPUT_TYPE_PASSED_GROUPS_ALL_RULES				= "passed groups - all rules";
    
    private static final String DEFAULT_BATCH_SIZE								= "100";
    
    // possible delimiter types
    private static final String SEPARATOR_COMMA									= ",";
    private static final String SEPARATOR_SEMICOLON								= ";";
    private static final String SEPARATOR_TAB									= "\t";
    
    // names for the processor state
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
            .allowableValues(FIELD_SEPARATOR_POSSIBLE_VALUE_COMMA,FIELD_SEPARATOR_POSSIBLE_VALUE_SEMICOLON, FIELD_SEPARATOR_POSSIBLE_VALUE_TAB)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specify the field separator to be used to split the incomming flow file content. The content must be a single row of CSV data. This separator is also used to split the fields of the header row.")
            .build();
    
    public static final PropertyDescriptor ATTRIBUTE_OUTPUT_DETAILED_RESULTS = new PropertyDescriptor.Builder()
            .name(RULEENGINE_OUTPUT_DETAILS)
            .required(true)
            .allowableValues("true","false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .description("Specify if the detailed results should be output. This creates flow files containing one row of data per rule (!). E.g. if you have one row of data and 10 rules, the flowfile contains 10 rows with the detailed results for each rule.")
            .build();

    public static final PropertyDescriptor ATTRIBUTE_OUTPUT_DETAILED_RESULTS_TYPE = new PropertyDescriptor.Builder()
            .name(RULEENGINE_OUTPUT_DETAILS_TYPE)
            .required(true)
            .allowableValues(OUTPUT_TYPE_ALL_GROUPS_ALL_RULES,OUTPUT_TYPE_FAILED_GROUPS_FAILED_RULES,OUTPUT_TYPE_FAILED_GROUPS_PASSED_RULES,OUTPUT_TYPE_FAILED_GROUPS_ALL_RULES,OUTPUT_TYPE_PASSED_GROUPS_FAILED_RULES,OUTPUT_TYPE_PASSED_GROUPS_PASSED_RULES, OUTPUT_TYPE_PASSED_GROUPS_ALL_RULES)
            .defaultValue(OUTPUT_TYPE_ALL_GROUPS_ALL_RULES)
            .description("Specify which detailed results should be output.")
            .build();

    public static final PropertyDescriptor ATTRIBUTE_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name(BATCH_SIZE_NAME)
            .required(true)
            .defaultValue(DEFAULT_BATCH_SIZE)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .description("Specify the batch size of rows to process. After the number of rows is reached the flow files are created.")
            .build();
    
    public static final Relationship ORIGINAL = new Relationship.Builder()
            .name(RELATIONSHIP_ORIGINAL_NAME)
            .description("The original flow file is routed to this relation.")
            .build();
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name(RELATIONSHIP_SUCESS_NAME)
            .description("The ruleengine results for each row of the incomming flow file are routed to this relation")
            .build();

    public static final Relationship DETAILED_RESULTS = new Relationship.Builder()
            .name(RELATIONSHIP_DETAILED_OUTPUT_NAME)
            .description("The ruleengine detailed results of the business rules execution according to the output detailed results type selection are routed to this relation")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name(RELATIONSHIP_FAILURE_NAME)
            .description("If the content could not be split or the ruleengine execution resulted in an error the file is routed to this relation.")
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
        properties.add(ATTRIBUTE_OUTPUT_DETAILED_RESULTS_TYPE);
        properties.add(ATTRIBUTE_BATCH_SIZE);
        this.properties = Collections.unmodifiableList(properties);

        // add relationships to set
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(ORIGINAL);
        relationships.add(SUCCESS);
        relationships.add(DETAILED_RESULTS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }
    
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws Exception
    {
        // faced problems when having \t as an attribute and could not find a solution
    	// so using allowable values instead and translating them here
    	if(context.getProperty(ATTRIBUTE_FIELD_SEPARATOR).getValue().equals(FIELD_SEPARATOR_POSSIBLE_VALUE_COMMA))
        {
        	separator = SEPARATOR_COMMA;
        }
        else if(context.getProperty(ATTRIBUTE_FIELD_SEPARATOR).getValue().equals(FIELD_SEPARATOR_POSSIBLE_VALUE_SEMICOLON))
        {
        	separator = SEPARATOR_SEMICOLON;
        }
        else if(context.getProperty(ATTRIBUTE_FIELD_SEPARATOR).getValue().equals(FIELD_SEPARATOR_POSSIBLE_VALUE_TAB))
        {
        	separator = SEPARATOR_TAB;
        }
    	// just in case...
        else
        {
        	separator = SEPARATOR_COMMA;
        }
    	
    	// if field names are specified
    	if(context.getProperty(ATTRIBUTE_FIELD_NAMES)!=null && !context.getProperty(ATTRIBUTE_FIELD_NAMES).getValue().equals(""))
    	{
    		// create a HeaderRow instance
    		headerFromFieldNames = new HeaderRow(context.getProperty(ATTRIBUTE_FIELD_NAMES).getValue(),FIELD_NAMES_SEPARATOR);
    		getLogger().debug("field names defined - fields found: [" + headerFromFieldNames.getNumberOfFields());
    	}
    	
    	// get the zip file, containing the business rules
        File file = new File(context.getProperty(ATTRIBUTE_RULEENGINE_ZIPFILE).getValue());
        if(file.exists() && file.isFile())
        {
        	// put filename and last modified date into hashmap
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
        	getLogger().debug("using project zip file: " + context.getProperty(ATTRIBUTE_RULEENGINE_ZIPFILE).getValue());
        	
        	try
        	{
            	// create ruleengine instance with the zip file
                ruleEngine = new BusinessRulesEngine(ruleEngineProjectFile);
                
                // output basic info about the ruleengine initialization
                getLogger().info("initialized business rule engine version: " + BusinessRulesEngine.getVersion() + " using " + context.getProperty(ATTRIBUTE_RULEENGINE_ZIPFILE).getValue() + ", last modified: [" + fileLastModifiedDateAsString + "]");
                // display the field seperator used
                getLogger().debug("field separator to split row into fields: " + context.getProperty(ATTRIBUTE_FIELD_SEPARATOR).getValue());
                // we display the number of rulegroups contained in the zip file
                getLogger().debug("number of rulegroups in project zip file: " + ruleEngine.getNumberOfGroups());

                // we do not want to keep the detailed results if they are not needed (no detailed results)
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
        getLogger().debug("processor unscheduled - ruleengine instance set to null");
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException 
    {
    	// map used to store the attribute name and its value from the content of the flow file
        final Map<String, String> propertyMap = new HashMap<>();
    	
        // get a logger instance
    	final ComponentLog logger = getLogger();
    	
    	// a header from the content if present
    	final AtomicReference<HeaderRow> header = new AtomicReference<>();
    	
    	AtomicBoolean error = new AtomicBoolean();
    	
    	// get the flow file
    	FlowFile flowFile = session.get();
        if (flowFile == null) 
        {
            return;
        }

        // list of rows from splitting the original flow file content
        ArrayList<RuleEngineRow> flowFileRows = new ArrayList<RuleEngineRow>();
        
        // list of rows containing the detailed results of the ruleengine
        ArrayList<RuleEngineRow> flowFileDetails = new ArrayList<RuleEngineRow>();
        
        boolean headerPresent = context.getProperty(ATTRIBUTE_HEADER_PRESENT).getValue().equals("true");
        
        // put the name of the ruleengine zip file in the list of properties
        propertyMap.put(PROPERTY_RULEENGINE_ZIPFILE_NAME, context.getProperty(ATTRIBUTE_RULEENGINE_ZIPFILE).getValue() );
        
        final int batchSize = Integer.parseInt(context.getProperty(BATCH_SIZE_NAME).getValue());
        
        // read flow file into input stream
        session.read(flowFile, new InputStreamCallback() 
        {
            public void process(InputStream in) throws IOException 
            {
                try 
                {
                	// iterator over the lines from the input stream
                	LineIterator iterator = IOUtils.lineIterator(in, "utf-8");
                	
                    // check if configuration indicates that a header row is present in the flow file content
                    if(headerPresent)
                    {
                    	logger.debug("configuration indicates a header row is present in flow file content");

                    	// if there is at least one row of data and the header is not defined yet
                    	if(iterator.hasNext() && header.get()==null)
                    	{
                    		// set the header from the content
	                    	header.set(new HeaderRow(iterator.nextLine(),separator));
                    	}
                    }
                    // if no header row is present in the flow file content
                    else
                    {
                    	logger.debug("configuration indicates no header row is present in flow file content");
                    	
                    	// use the header from the field names
                    	header.set(headerFromFieldNames);
                    }
                    
                    // loop over all rows of data
                    while(iterator.hasNext())
                    {
                    	// we handle the error per row of data
                    	error.set(false);
                    	
                    	// get a row to process
                    	String row = iterator.nextLine();
                    	
	                    // check that we have data
	                    if (row != null && !row.trim().equals("")) 
	                    {
	                    	RowFieldCollection rowFieldCollection = null;
	                    	try
	                		{
	                    		// create RowFieldCollection from the row and the header fields
	                    		rowFieldCollection = getRowFieldCollection(row,header.get());
	
	                    		logger.debug("RowFieldCollection header contains: " + rowFieldCollection.getHeader().getNumberOfFields() + " fields");
		                		logger.debug("RowFieldCollection contains: " + rowFieldCollection.getNumberOfFields() + " fields");
		                		
		        		        // run the ruleengine with the given data from the flow file
		                		logger.debug("running business ruleengine...");

		                		// run the business logic/rules against the data
	                			ruleEngine.run("flowfile",rowFieldCollection);
		                		
	                			// add some debugging output that might be useful
		        		        logger.debug("number of rulegroups: " + ruleEngine.getNumberOfGroups());
		        		        logger.debug("number of rulegroups passed: " + ruleEngine.getNumberOfGroupsPassed());
		        		        logger.debug("number of rulegroups failed: " + ruleEngine.getNumberOfGroupsFailed());
		        		        logger.debug("number of rulegroups skipped: " + ruleEngine.getNumberOfGroupsSkipped());
		        		        logger.debug("number of rules: " + ruleEngine.getNumberOfRules());
		        		        logger.debug("number of rules passed: " + ruleEngine.getNumberOfRulesPassed());
		        		        logger.debug("number of rules failed: " + ruleEngine.getNumberOfRulesFailed());
		        		        logger.debug("number of actions: " + ruleEngine.getNumberOfActions());
		        		    	
		        		        // add some properties of the ruleengine execution to the map
		        		        addRuleEngineProperties(propertyMap);
	                		}
	                		catch(Exception ex)
	                		{
	                			error.set(true);
	                			logger.error(ex.getMessage(), ex);
	                		}

	                    	// if no error occurred we create a save the data for the creation of the flow files
	        		        if(!error.get())
	        		        {
		                		// process only if the collection of fields was changed by
		        		        // a ruleengine action. this means the data was updated so
		        		        // we will have to re-write/re-create the flow file content.
		       		        	if(rowFieldCollection.isCollectionUpdated())
		        		        {
		       		        		// put an indicator that the data was modified by the ruleengine
		       		        		propertyMap.put(PROPERTY_RULEENGINE_CONTENT_MODIFIED, "true");
		       		        		
		       		        		logger.debug("data was modified - updating flow file content with ruleengine results");
	 		       	        		
		       		        		// the RuleEngineRow instance will contain the row of data and the map of properties
		       		        		// and will later be used when the flow files are created
		       		        		flowFileRows.add(new RuleEngineRow(getResultRow(rowFieldCollection),propertyMap));
		       		        	}
		       		        	else
		       		        	{
		       		        		// put an indicator that the data was NOT modified by the ruleengine
		       		        		propertyMap.put(PROPERTY_RULEENGINE_CONTENT_MODIFIED, "false");
		       		        		
		       		        		logger.debug("data was not modified - using original content");
	
		       		        		// the RuleEngineRow instance will contain the row of data and the map of properties
		       		        		// and will later be used when the flow files are created
		       		        		flowFileRows.add(new RuleEngineRow(row,propertyMap)); 
		       		        	}
		       		        	
		       		        	if(flowFileRows.size()>=batchSize)
	     	            	   	{
		       		        		// generate flow files from the individual rows
		       		        		List<FlowFile> splitFlowFiles = generateFlowFileSplits(context,session,flowFileRows, header.get(), headerPresent);
		       		        		// transfer all individual rows to success relationship
		       		        		if(splitFlowFiles.size()>0)
		       		        		{
		       		        			session.transfer(splitFlowFiles, SUCCESS);
		       		        		}
	     	            	   	}
		       		        	
		                        // if the user configured detailed results 
		                        if(context.getProperty(ATTRIBUTE_OUTPUT_DETAILED_RESULTS).getValue().equals("true"))
		                        {
		                        	// get the configured output type
		     		               	String outputType = context.getProperty(ATTRIBUTE_OUTPUT_DETAILED_RESULTS_TYPE).getValue(); 
		     		               	logger.debug("configuration set to output detailed results with type [" + outputType + "]");
	
		     		               	// we need to create a flow file only, if the ruleengine results are according to the output type settings
			     		           	if(outputType.equals(OUTPUT_TYPE_ALL_GROUPS_ALL_RULES) 
			     		           			|| (outputType.equals(OUTPUT_TYPE_FAILED_GROUPS_ALL_RULES) && ruleEngine.getNumberOfGroupsFailed()>0) 
			     		           			|| (outputType.equals(OUTPUT_TYPE_FAILED_GROUPS_FAILED_RULES) && ruleEngine.getNumberOfGroupsFailed()>0) 
			     		           			|| (outputType.equals(OUTPUT_TYPE_FAILED_GROUPS_PASSED_RULES) && ruleEngine.getNumberOfGroupsFailed()>0) 
			     		           			|| (outputType.equals(OUTPUT_TYPE_PASSED_GROUPS_ALL_RULES) && ruleEngine.getNumberOfGroupsPassed()>0) 
			     		           			|| (outputType.equals(OUTPUT_TYPE_PASSED_GROUPS_FAILED_RULES) && ruleEngine.getNumberOfGroupsPassed()>0 
			     		           			|| (outputType.equals(OUTPUT_TYPE_PASSED_GROUPS_PASSED_RULES) && ruleEngine.getNumberOfGroupsPassed()>0)))
			     		           	{
			     		               	// create the content for the flow file
			     		               	String content = getFlowFileRuleEngineDetailsContent(header.get(), headerPresent, outputType, row);
			     		               	
			     	            	   	// add results to the list
			     	            	   	flowFileDetails.add(new RuleEngineRow(content,propertyMap));
			     	            	   	
			     	            	   	if(flowFileDetails.size()>=batchSize)
			     	            	   	{
			     	            	   		List<FlowFile> detailsFlowFiles = generateFlowFilesRuleEngineDetails(context,session,flowFileDetails, header.get(), headerPresent);
			     	            	   		// transfer all individual rows to detailed relationship
			     	            	   		if(detailsFlowFiles.size()>0)
			     	            	   		{
			     	            	   			session.transfer(detailsFlowFiles, DETAILED_RESULTS);
			     	            	   		}
			     	            	   	}
			     		           	}
		                        }
		        		        // clear the collections of ruleengine results
		        		    	ruleEngine.getRuleExecutionCollection().clear();
	        		        }
	        		        // if we have an error we create a flow file from the current row of data and send it to the failure relationsship
	        		        else
	        		        {
	        		        	FlowFile failureFlowFile = generateFailureFlowFile(context,session,row,header.get(),headerPresent);
	        		        	session.transfer(failureFlowFile,FAILURE);
	        		        }
	                    }
	                }
                    
                    LineIterator.closeQuietly(iterator);
                }
                catch (Exception ex) 
                {
                    ex.printStackTrace();
                    logger.error("error running the business ruleengine",ex);
                }
            }
        });

        // generate flow files from the individual rows
        List<FlowFile> splitFlowFiles = generateFlowFileSplits(context,session,flowFileRows, header.get(), headerPresent);
        
        // generate flow files from the individual rows
        List<FlowFile> detailsFlowFiles = generateFlowFilesRuleEngineDetails(context,session,flowFileDetails, header.get(), headerPresent);

       	// transfer the original flow file
       	session.transfer(flowFile,ORIGINAL);

       	// transfer all individual rows to success relationship
        if(splitFlowFiles.size()>0)
        {
        	session.transfer(splitFlowFiles, SUCCESS);
        }
        
        // transfer all individual rows to success relationship
        if(detailsFlowFiles.size()>0)
        {
        	session.transfer(detailsFlowFiles, DETAILED_RESULTS);
        }
    }
    
    /**
     * generates a failure flow file for the current row of data
     *  
     * @param context			process context
     * @param session			process session
     * @param row				the current row of data from the flow file content
     * @param header			the header row
     * @param headerPresent		indicator from the configuration if a header is present
     * @return
     */
    private FlowFile generateFailureFlowFile(ProcessContext context, ProcessSession session, String row, HeaderRow header, boolean headerPresent)
    {
		FlowFile failureFlowFile = session.create();
		failureFlowFile = updateFailureFlowFileContent(header, headerPresent, context, session, failureFlowFile, row);
		
    	return failureFlowFile;
    }
    
    /**
     * updates the content of the failure flow file a row of data
     * 
     * @param header			the header row
     * @param headerPresent		indicator from the configuration if a header is present
     * @param context			process context
     * @param session			process session
     * @param someFlowFile		the flow file to update
     * @param content			a row of CSV data
     * @return
     */
    private FlowFile updateFailureFlowFileContent(HeaderRow header, boolean headerPresent, final ProcessContext context, final ProcessSession session, FlowFile someFlowFile, String content)
    {
    	// write data to output stream
    	someFlowFile = session.write(someFlowFile, new OutputStreamCallback() 
    	{
           @Override
           public void process(final OutputStream out) throws IOException 
           {
        	   // buffer to hold the row data/content
        	   StringBuffer buffer = new StringBuffer();

        	   // append the header row if present
        	   if(headerPresent && header!=null && header.getNumberOfFields()>0)
        	   {
					String headerRow = header.getFieldNamesWithSeparator();
					buffer.append(headerRow); 
					// if the header row does not have a line separator at the end then add it
					if(!headerRow.endsWith(System.lineSeparator()))
					{ 
						buffer.append(System.lineSeparator());
					}
        	   }
				   
        	   // append to the buffer
        	   if(content!=null)
        	   {
        		   buffer.append(content);
        	   }
        	   final byte[] data = buffer.toString().getBytes();
        	   out.write(data);
           }
    	});
    	return someFlowFile;
    }
    
    /**
     * generates the flow files for each row of data in the flow file in the form of a list
     * 
     * @param context			process context
     * @param session			process session
     * @param rows				list of rows from the flow file content
     * @param header			the header row
     * @param headerPresent		indicator from the configuration if a header is present
     * @param propertyMap		map of properties of the flow file
     * @return					list of flow files
     */
    private List<FlowFile> generateFlowFileSplits(ProcessContext context, ProcessSession session, ArrayList<RuleEngineRow> rows, HeaderRow header, boolean headerPresent)
    {
    	List<FlowFile> splitFlowFiles = new ArrayList<>();
    	
    	for(int i=0;i<rows.size();i++)
    	{
    		FlowFile splitFlowFile = session.create();
    		splitFlowFile = updateFlowFileContent(header, headerPresent, context, session, splitFlowFile, rows.get(i));
    		
    		// put the properties in the flow file
    		splitFlowFile = session.putAllAttributes(splitFlowFile, rows.get(i).getMap());

    		splitFlowFiles.add(splitFlowFile);
    	}
    	rows.clear();
    	
    	getLogger().debug("created list of "+ splitFlowFiles.size() + " flowfiles");
    	return splitFlowFiles;
    }
    
    
    
    /**
     * updates the content of the flow file for each row of data
     * 
     * @param header			the header row
     * @param headerPresent		indicator from the configuration if a header is present
     * @param context			process context
     * @param session			process session
     * @param someFlowFile		the flow file to update
     * @param content			the content to write to the flow file
     * @param propertyMap		map of properties of the flow file
     * @return					a flow file
     */
    private FlowFile updateFlowFileContent(HeaderRow header, boolean headerPresent, final ProcessContext context, final ProcessSession session, FlowFile someFlowFile, RuleEngineRow content)
    {
    	// write data to output stream
    	someFlowFile = session.write(someFlowFile, new OutputStreamCallback() 
    	{
           @Override
           public void process(final OutputStream out) throws IOException 
           {
        	   // buffer to hold the row data/content
        	   StringBuffer buffer = new StringBuffer();

        	   // append the header row if present
        	   if(headerPresent && header!=null && header.getNumberOfFields()>0)
        	   {
					String headerRow = header.getFieldNamesWithSeparator();
					buffer.append(headerRow); 
					// if the header row does not have a line separator at the end then add it
					if(!headerRow.endsWith(System.lineSeparator()))
					{ 
						buffer.append(System.lineSeparator());
					}
        	   }
				   
        	   // append to the buffer
        	   if(content.getRow()!=null)
        	   {
        		   buffer.append(content.getRow());
        	   }
        	   final byte[] data = buffer.toString().getBytes();
        	   out.write(data);
           }
    	});
    	return someFlowFile;
    }
    
    /**
     * generates the flow files for each row of data for the ruleengine details in the flow file - in the form of a list
     * 
     * @param context			process context
     * @param session			process session
     * @param detailsRows		array of rows containing the data row and a property map
     * @param header			the header from the flow file content or the given field names
     * @param headerPresent		indicator from the configuration if a header is present
     * @return					a list of flow files
     */
    private List<FlowFile> generateFlowFilesRuleEngineDetails(ProcessContext context, ProcessSession session, ArrayList<RuleEngineRow> detailsRows, HeaderRow header, boolean headerPresent)
    {
    	List<FlowFile> detailsFlowFiles = new ArrayList<>();

    	for(int i=0;i<detailsRows.size();i++)
    	{
			FlowFile detailsFlowFile = session.create();
			detailsFlowFile = updateFlowFileRuleEngineDetailsContent(header, headerPresent, context, session, detailsFlowFile, detailsRows.get(i));
		
			// use the attributes of the original flow file
			detailsFlowFile = session.putAllAttributes(detailsFlowFile, detailsRows.get(i).getMap());
			
			detailsFlowFiles.add(detailsFlowFile);
    	}
    	detailsRows.clear();
    	
    	getLogger().debug("created list of "+ detailsFlowFiles.size() + " ruleengine details flowfiles");
    	return detailsFlowFiles;
    }
    
    /**
     * writes the updated content from the ruleengine to the flow file content.
     * 
     * @param header				the header from the content or the field names from the configuration
     * @param headerPresent			indicator from the configuration if a header is present in the flow file content
     * @param context				process context
     * @param session				process session
     * @param someFlowFile			the flow file
     * @param content				a row of CSV data
     * @return						a flow file with updated content
     */
    private FlowFile updateFlowFileRuleEngineDetailsContent(HeaderRow header, boolean headerPresent, final ProcessContext context, final ProcessSession session, FlowFile someFlowFile, RuleEngineRow content)
    {
    	if(content.getRow()!=null)
    	{
	    	// write data to output stream
	    	someFlowFile = session.write(someFlowFile, new OutputStreamCallback() 
	    	{
	               @Override
	               public void process(final OutputStream out) throws IOException 
	               {
	            	   	// write data to content
	   					final byte[] data = content.getRow().getBytes();
	   					out.write(data);
	               }
	        });
    	}
	    return someFlowFile;
    }
    
    /**
     * construct the content for the flow file for the rule engine details.
     * 
     * when the rule engine runs, it produces results per data row and business rules. with 1 data row and 10 rules you get 10 detailed results. these results are output
     * in one flow file to the details relationship.
     * 
     * @param header
     * @param headerPresent
     * @param outputType
     * @param row
     * @return
     */
    private String getFlowFileRuleEngineDetailsContent(HeaderRow header, boolean headerPresent, String outputType, String row)
    {
    	StringBuffer detailedFlowFileContent = new StringBuffer();	
  	   
        // add the header if there is one
 	   	if(headerPresent && header!=null && header.getNumberOfFields()>0)
        {
        	String headerRow = header.getFieldNamesWithSeparator();
        	detailedFlowFileContent.append(headerRow);

        	// if the header row does not have a line separator at the end then add it
        	if(!headerRow.endsWith(System.lineSeparator()))
        	{ 
        		detailedFlowFileContent.append(System.lineSeparator());
        	}
        }
 	   	
 	   	// append the results
 	   	detailedFlowFileContent.append(getRuleEngineDetails(outputType,row));
    	
    	return detailedFlowFileContent.toString();
    }
    
    /**
     * creates a RowFieldCollection object from a row of data and a header row. the header can come from a header in the
     * content or by specifying the names of the individual fields in the configuration.
     * 
     * if the header is null then the collection is built without a header. in this case the access to the fields from
     * the rules needs to be constructed using the field index number (because we do not have the name of the field).
     * 
     * @param row			a row of CSV data
     * @param header		a HeaderRow object containing the header fields
     * @return				a RowFieldCollection with fields
     * @throws Exception	exception when the fields can not be constructed
     */
    private RowFieldCollection getRowFieldCollection(String row, HeaderRow header) throws Exception
    {
    	// use the Splitter class to split the incoming row into fields according to the given separator
    	// pass info which separator is used
		Splitter splitter = new Splitter(Splitter.TYPE_COMMA_SEPERATED,separator);

		// put the values of the fields of the CSV row into a collection
		if(header!=null)
		{
			return new RowFieldCollection(header, splitter.getFields(row));
		}
		else
		{
			return new RowFieldCollection(splitter.getFields(row));
		}
    }
    
    /**
     * when the ruleengine ran, a collection of results is available - if the rules passed or failed, how many failed, how many groups failed,
     * how many actions failed, a message,  etc.
     * 
     * for a given row of CSV data this method produces one row per business rule. each row will contain information about the rule execution.
     * 
     * @param outputType		the output type configured in the configuration
     * @param content			a row of CSV data
     * @return					the original row multiplied with the number of rules
     */
    private String getRuleEngineDetails(String outputType, String content)
    {
    	StringBuffer row = new StringBuffer();
    	
    	// loop over all groups
	   	for(int f=0;f<ruleEngine.getGroups().size();f++)
        {
	   		// get a rulegroup
        	RuleGroup group = ruleEngine.getGroups().get(f);
        	
        	try
        	{
            	// loop over all subgroups of the group
        		for(int g=0;g<group.getSubGroups().size();g++)
                {
            		RuleSubGroup subgroup = group.getSubGroups().get(g);
            		// get the execution results of the subgroup
            		ArrayList <RuleExecutionResult> results = subgroup.getExecutionCollection().getResults();
            		// loop over all results
            		for (int h = 0;h < results.size();h++)
                    {
            			// one result of one rule execution
            			RuleExecutionResult result = results.get(h);
            			// the corresponding rule that was executed
            			XmlRule rule = result.getRule();
	   	
            			// check if rulegroup and rule are according to the output type settings
            			// otherwise we don't need to output the data
            			if(outputType.equals(OUTPUT_TYPE_ALL_GROUPS_ALL_RULES) 
            					|| (outputType.equals(OUTPUT_TYPE_FAILED_GROUPS_FAILED_RULES) && group.getFailed()==1 && rule.getFailed()==1) 
            					|| (outputType.equals(OUTPUT_TYPE_FAILED_GROUPS_PASSED_RULES) && group.getFailed()==1 && rule.getFailed()==0)
            					|| outputType.equals(OUTPUT_TYPE_FAILED_GROUPS_ALL_RULES) && group.getFailed()==1 
            					|| (outputType.equals(OUTPUT_TYPE_PASSED_GROUPS_FAILED_RULES) && group.getFailed()==0 && rule.getFailed()==1)  
            					|| (outputType.equals(OUTPUT_TYPE_PASSED_GROUPS_PASSED_RULES) && group.getFailed()==0 && rule.getFailed()==0)
            					|| (outputType.equals(OUTPUT_TYPE_PASSED_GROUPS_ALL_RULES) && group.getFailed()==0))
            			{
            				// append the content 
            				row.append(content);
   				        	// add a separator
            				row.append(separator);
	   					    
	   					    // append information about the ruleengine execution to each flow file
	   					    // so that the rulegroup, subgroup and rule can be identified
            				row.append( group.getId());
            				row.append(separator);
	   				        
            				row.append( group.getFailed());
            				row.append(separator);
	   					    
            				row.append( subgroup.getId());
            				row.append(separator);
	   					    
            				row.append( subgroup.getFailed());
            				row.append(separator);
	   					    
            				row.append( subgroup.getLogicalOperatorSubGroupAsString());
            				row.append(separator);
	   					    
            				row.append(subgroup.getLogicalOperatorRulesAsString());
            				row.append(separator);
	   					    
            				row.append(rule.getId());
            				row.append(separator);
	   				        
            				row.append(rule.getFailed());
            				row.append(separator);
	   					    
	   					    // append the resulting ruleengine message of the rule execution
            				row.append(result.getMessage());
	   					    
	   					    // add line separator except last line
	   					    if(h<results.size()-1 || g<group.getSubGroups().size()-1)
	   					    {
	   					    	row.append(System.lineSeparator());
	   					    }
            			}
                    }
            	}
        	}
        	catch(Exception ex)
        	{
        		ex.printStackTrace();
        	}
		}
	   	return row.toString();
    }

    /**
     * re-constructs a row of CSV data from the RowFieldCollecion.
     * 
     * when the ruleengine runs, actions may update the data based on the results of the business rules. this method
     * re-constructs the original row with the updated values from the rule engine and the given separator.     * 
     * 
     * @param rowFieldCollection	RowFieldCollection that was used with the ruleengine run
     * @return						a row of data with the updated data
     * @throws Exception			exception when the row can not be constructed
     */
    private String getResultRow(RowFieldCollection rowFieldCollection) throws Exception
    {
    	StringBuffer content = new StringBuffer();
    	// loop through the collection and construct the output row
        // using the separator that was used to split it previously
   		for(int i=0;i<rowFieldCollection.getFields().size();i++)
        {
       		RowField rf = rowFieldCollection.getField(i);
       		// append the field value - the fields might have been updated by an action 
       		content.append(rf.getValue());
   			if(i<rowFieldCollection.getFields().size()-1)
   			{
   				// append the separator
   				content.append(separator);
   			}
        }
   		return content.toString();
    }
    
    /**
     * adds properties to the given map which contain statistics about the rule engine execution.
     * 
     * these properties will be available as flow file attributes.
     * 
     * @param propertyMap		the flow file properties
     * @throws Exception		exception when the ruleengine values can not be determined
     */
    private void addRuleEngineProperties(Map<String, String> propertyMap) throws Exception
    {
        // put the  number of  rulegroups in the property map
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
