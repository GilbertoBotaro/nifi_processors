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
import com.datamelt.rules.core.RuleGroup;
import com.datamelt.rules.core.RuleSubGroup;
import com.datamelt.rules.core.XmlRule;
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

 * @author uwe geercken - last update 2017-03-14
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
public class ExecuteRuleEngine extends AbstractProcessor 
{
    // list of properties
	private List<PropertyDescriptor> properties;
	// set of relationships
    private Set<Relationship> relationships;

    private String separator;
    
    // collection of field names (header) and field values
    private RowFieldCollection rowFieldCollection;
    
    // the list of field names to use
    private static String [] fieldNames;
    
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
    private static final String HEADER_PRESENT_PROPERTY_NAME 					= "Header present";
    private static final String FIELD_NAMES_PROPERTY_NAME 						= "Field Names";
    private static final String RULEENGINE_OUTPUT_DETAILS 						= "Output detailed results";
    private static final String RULEENGINE_OUTPUT_DETAILS_TYPE					= "Output detailed results type";
    
    // relationships
    private static final String RELATIONSHIP_SUCESS_NAME 						= "success";
    private static final String RELATIONSHIP_DETAILED_OUTPUT_NAME				= "detailed output";

    // separator used to split up the defined field names
    private static final String FIELD_NAMES_SEPARATOR 							= ",";
    
    private static final String FIELD_SEPERATOR_POSSIBLE_VALUE_COMMA			= "Comma";
    private static final String FIELD_SEPERATOR_POSSIBLE_VALUE_SEMICOLON		= "Semicolon";
    private static final String FIELD_SEPERATOR_POSSIBLE_VALUE_TAB				= "Tab";
    
    
    private static final String OUTPUT_TYPE_ALL_GROUPS_ALL_RULES				= "all groups - all rules";
    private static final String OUTPUT_TYPE_FAILED_GROUPS_FAILED_RULES			= "failed groups - failed rules only ";
    private static final String OUTPUT_TYPE_FAILED_GROUPS_PASSED_RULES			= "failed groups - passed rules only ";
    private static final String OUTPUT_TYPE_FAILED_GROUPS_ALL_RULES				= "failed groups - all rules";
    private static final String OUTPUT_TYPE_PASSED_GROUPS_FAILED_RULES			= "passed groups - failed rules only";
    private static final String OUTPUT_TYPE_PASSED_GROUPS_PASSED_RULES			= "passed groups - passed rules only";
    private static final String OUTPUT_TYPE_PASSED_GROUPS_ALL_RULES				= "passed groups - all rules";
    
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

    public static final PropertyDescriptor ATTRIBUTE_OUTPUT_DETAILED_RESULTS_TYPE = new PropertyDescriptor.Builder()
            .name(RULEENGINE_OUTPUT_DETAILS_TYPE)
            .required(true)
            .allowableValues(OUTPUT_TYPE_ALL_GROUPS_ALL_RULES,OUTPUT_TYPE_FAILED_GROUPS_FAILED_RULES,OUTPUT_TYPE_FAILED_GROUPS_PASSED_RULES,OUTPUT_TYPE_FAILED_GROUPS_ALL_RULES,OUTPUT_TYPE_PASSED_GROUPS_FAILED_RULES,OUTPUT_TYPE_PASSED_GROUPS_PASSED_RULES, OUTPUT_TYPE_PASSED_GROUPS_ALL_RULES)
            .defaultValue(OUTPUT_TYPE_ALL_GROUPS_ALL_RULES)
            .description("Specify which detailed results should be output.")
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name(RELATIONSHIP_SUCESS_NAME)
            .description("The ruleengine successfully executed the business rules against the flow file content.")
            .build();

    public static final Relationship DETAILED_RESULTS = new Relationship.Builder()
            .name(RELATIONSHIP_DETAILED_OUTPUT_NAME)
            .description("The ruleengine detailed results of the business rules execution.")
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
    	// just in case...
        else
        {
        	separator = SEPERATOR_COMMA;
        }
    	
    	// create a RowFieldCollection
    	rowFieldCollection = new RowFieldCollection();
    	
    	if(context.getProperty(ATTRIBUTE_FIELD_NAMES)!=null && !context.getProperty(ATTRIBUTE_FIELD_NAMES).getValue().equals(""))
    	{
    		// split the field names into an array
        	fieldNames = context.getProperty(ATTRIBUTE_FIELD_NAMES).getValue().split(FIELD_NAMES_SEPARATOR);
        	rowFieldCollection.setFieldNames(fieldNames);
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
    	
    	final AtomicReference<String> headerRow = new AtomicReference<>();
    	
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
                    String row = null;
                    
                    // split the content into rows - might be multiple ones
                    String[] originalContentRows = originalContent.split(System.lineSeparator());
                    
                    // check if configuration indicates that a header row is present
                    if(context.getProperty(ATTRIBUTE_HEADER_PRESENT).getValue().equals("true"))
                    {
                    	logger.debug("configuration indicates a (1) header row is present");
                    	
                    	// we have two rows of data
                    	if(originalContentRows!=null && originalContentRows.length==2)
                    	{
                    		logger.debug("found 2 rows in flow file content - assuming 1 header row and 1 data row");
                    		// first row is the header row
                    		headerRow.set(originalContentRows[0]);
                    		// second row is the data
                    		row = originalContentRows[1];
                    	}
                    	// we have one row of data
                    	else if(originalContentRows!=null && originalContentRows.length==1)
                    	{
                    		logger.debug("found 1 row in flow file content - assuming 1 header row");
                    		// first row is the header row but no further rows found (no data)
                    		headerRow.set(originalContentRows[0]);
                    	}
                    	// we have no rows of data
                    	else if(originalContentRows!=null && originalContentRows.length==0)
                    	{
                    		// flow file has no rows - is empty
                    		logger.debug("found 0 rows in flow file content");
                    	}
                    	// we have more than two rows of data
                    	// this should not happen. we use the two rows - 1 row header and 1 row data - and ignore the rest of the rows.
                    	// the ruleengine works on single rows of data (or single objects), so multiple data rows will not work well in
                    	// the scenario of flow files
                    	else
                    	{
                    		if(originalContentRows!=null)
                    		{
                    			logger.warn("found more than 2 rows in flow file content - assuming header row and data row and ignoring additional rows");
                    			// first row is the header row
                    			headerRow.set(originalContentRows[0]);
                    			// second row is the data
                    			row = originalContentRows[1];
                    		}
                    	}
                    }
                    // if no header row is present
                    else
                    {
                    	logger.debug("configuration indicates no header row is present");
                    	
                    	// we have one row of data
                    	if(originalContentRows!=null && originalContentRows.length==1)
                    	{
                    		// first (and only) row is the data
                    		row = originalContentRows[0];
                    	}
                    	// we have no rows of data
                    	else if(originalContentRows!=null && originalContentRows.length==0)
                    	{
                    		// flow file has no rows - is empty
                    		logger.debug("found 0 rows in flow file content");
                    	}
                    	// we have more than one row of data
                    	// -- see comment further above --
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
                        
                        // use the Splitter class to split the incoming row into fields according to the given separator
                    	// pass info which separator is used
                		Splitter splitter = new Splitter(Splitter.TYPE_COMMA_SEPERATED,separator);
                		logger.debug("created Splitter object using separator: [" + separator + "]");

                		// if we have a header row split it into its fields and put it in the collection
                		if(headerRow.get()!=null && !headerRow.get().trim().equals(""))
                		{
                			String[] headerFields = headerRow.get().split(separator);
                			rowFieldCollection.setFieldNames(headerFields);
                			logger.debug("splitted header row into fields: " + headerFields.length + " number of fields");
                		}

                		// put the values of the fields of the CSV row into a collection
                		rowFieldCollection.setFields(splitter.getFields(row)); 
                		logger.debug("RowFieldCollection contains: " + rowFieldCollection.getNumberOfFields() + " number of fields");
                		
        		        // run the ruleengine with the given data from the flow file
                		logger.debug("running business ruleengine...");
                		ruleEngine.run("flowfile",rowFieldCollection);
        		        
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
        		        
        		        // add some properties of the ruleengine execution
        		        addRuleEngineProperties(propertyMap, ruleEngine);
        		        
        		        // process only if the collection of fields was changed by
        		        // a ruleengine action. this means the data was updated so
        		        // we have to re-write/re-create the flow file content.
        		        // if a header is present, then we have to put the header in the content first.
       		        	if(rowFieldCollection.isCollectionUpdated())
        		        {
       		        		collectionUpdated.set(true);
       		        		logger.debug("data was modified by an ruleengine action - reconstructing content");

       		        		// put an indicator that the data was modified by the ruleengine
       		        		propertyMap.put(PROPERTY_RULEENGINE_CONTENT_MODIFIED, "true");
       		        	}
       		        	else
       		        	{
       		        		// put an indicator that the data was NOT modified by the ruleengine
       		        		propertyMap.put(PROPERTY_RULEENGINE_CONTENT_MODIFIED, "false");
       		        	}

                    }
                }
                catch (Exception ex) 
                {
                    ex.printStackTrace();
                    logger.error("error running the business ruleengine",ex);
                }
            }
        });

        // variable to keep the updated data row
    	AtomicReference<String> updatedContent = new AtomicReference<String>();

    	// if data has changed or we output the details, re-create the content
        if(collectionUpdated.get()==true || context.getProperty(ATTRIBUTE_OUTPUT_DETAILED_RESULTS).getValue().equals("true"))
        {
        	try
        	{
        		// get the updated row from the collection
        		updatedContent.set(getResultRow(rowFieldCollection));
        	}
        	catch(Exception ex)
        	{
        		logger.error("error re-creating the data from the rowfieldcollection");
        	}
        }

        // if the data was updated (by an action)
        if(collectionUpdated.get()==true)
        {
        	// write data to output stream
        	flowFile = session.write(flowFile, new OutputStreamCallback() 
        	{
                   @Override
                   public void process(final OutputStream out) throws IOException 
                   {
  		        		// buffer to hold the row data/content
                	   StringBuffer content = new StringBuffer();

                	   // append the header row if present
	       		        if(context.getProperty(ATTRIBUTE_HEADER_PRESENT).getValue().equals("true") && headerRow!=null && !headerRow.get().trim().equals(""))
	       		        {
	       		        	content.append(headerRow);
	       		        	// if the header row does not have a line separator at the end then add it
	       		        	if(!headerRow.get().endsWith(System.lineSeparator()))
	       		        	{
	       		        		content.append(System.lineSeparator());
	       		        	}
	       		        }
	       		       
	       		       // the data was changed. we reconstruct the original row and using the defined separator
       		    	   // append to the buffer
       		    	   content.append(updatedContent.get());
                	   
	       		       final byte[] data = content.toString().getBytes();
                	   out.write(data);
                   }
               });
        }
    	
        // put the map to the flow file
        flowFile = session.putAllAttributes(flowFile, propertyMap);
        
        // if the user wants detailed results, then we clone the flow file, add the ruleengine message for 
        // each rule to the properties and transmit the flow file to the detailed relationship
        if(context.getProperty(ATTRIBUTE_OUTPUT_DETAILED_RESULTS).getValue().equals("true"))
        {
			// get the configured output type
        	String ouputType = context.getProperty(ATTRIBUTE_OUTPUT_DETAILED_RESULTS_TYPE).getValue(); 
        	
        	// we need to create a flow file only, if the ruleengine results are according to the output type settings
        	if(ouputType.equals(OUTPUT_TYPE_ALL_GROUPS_ALL_RULES) || (ouputType.equals(OUTPUT_TYPE_FAILED_GROUPS_ALL_RULES) && ruleEngine.getNumberOfGroupsFailed()>0) || (ouputType.equals(OUTPUT_TYPE_FAILED_GROUPS_FAILED_RULES) && ruleEngine.getNumberOfGroupsFailed()>0) || (ouputType.equals(OUTPUT_TYPE_FAILED_GROUPS_PASSED_RULES) && ruleEngine.getNumberOfGroupsFailed()>0) || (ouputType.equals(OUTPUT_TYPE_PASSED_GROUPS_ALL_RULES) && ruleEngine.getNumberOfGroupsPassed()>0) || (ouputType.equals(OUTPUT_TYPE_PASSED_GROUPS_FAILED_RULES) && ruleEngine.getNumberOfGroupsPassed()>0 || (ouputType.equals(OUTPUT_TYPE_PASSED_GROUPS_PASSED_RULES) && ruleEngine.getNumberOfGroupsPassed()>0)))
        	{
        		// create a clone of the original flow file
				FlowFile detailedFlowFile = session.clone(flowFile);
	
				// append a separator and the ruleengine message from the rule execution
				detailedFlowFile = session.write(detailedFlowFile, new OutputStreamCallback() 
	        	{
	                   @Override
	                   public void process(final OutputStream out) throws IOException 
	                   {
	                	   	StringBuffer detailedFlowFileContent = new StringBuffer();
	                	   	
	                	   	// add the header if there is one
	                	   	if(headerRow.get() !=null)
	                	   	{
	                	   		detailedFlowFileContent.append(headerRow.get());
	                	   	}
	                	   	
	                	   	// loop over all groups
	                	   	for(int f=0;f<ruleEngine.getGroups().size();f++)
	        	            {
	                	   		// get a rulegroup
	        	            	RuleGroup group = ruleEngine.getGroups().get(f);
	        	            	
	        	            	try
	        	            	{
	        		            	// loop over all subgroups
	        		        		for(int g=0;g<group.getSubGroups().size();g++)
	        		                {
	        		            		RuleSubGroup subgroup = group.getSubGroups().get(g);
	        		            		// get the execution results of the subgroup
	        		            		ArrayList <RuleExecutionResult> results = subgroup.getExecutionCollection().getResults();
	        		            		// loop over all results
	        		            		for (int h= 0;h< results.size();h++)
	        		                    {
	        		            			RuleExecutionResult result = results.get(h);
	        		            			XmlRule rule = result.getRule();
	                	   	
	        		            			// check if rulegroup and rule are according to the output type settings
	        		            			if(ouputType.equals(OUTPUT_TYPE_ALL_GROUPS_ALL_RULES) || (ouputType.equals(OUTPUT_TYPE_FAILED_GROUPS_FAILED_RULES) && group.getFailed()==1 && rule.getFailed()==1) || (ouputType.equals(OUTPUT_TYPE_FAILED_GROUPS_PASSED_RULES) && group.getFailed()==1 && rule.getFailed()==0)|| ouputType.equals(OUTPUT_TYPE_FAILED_GROUPS_ALL_RULES) && group.getFailed()==1 || (ouputType.equals(OUTPUT_TYPE_PASSED_GROUPS_FAILED_RULES) && group.getFailed()==0 && rule.getFailed()==1)  || (ouputType.equals(OUTPUT_TYPE_PASSED_GROUPS_PASSED_RULES) && group.getFailed()==0 && rule.getFailed()==0) || (ouputType.equals(OUTPUT_TYPE_PASSED_GROUPS_ALL_RULES) && group.getFailed()==0))
	        		            			{
	        		            				// append the content 
	        		   				        	detailedFlowFileContent.append(updatedContent.get());
	        		   				        	// add a separator
	        			   					    detailedFlowFileContent.append(separator);
	        			   					    
	        			   					    detailedFlowFileContent.append( group.getId());
	        			   					    detailedFlowFileContent.append(separator);
	        			   				        
	        			   					    detailedFlowFileContent.append( group.getFailed());
	        			   					    detailedFlowFileContent.append(separator);
	        			   					    
	        			   					    detailedFlowFileContent.append( subgroup.getId());
	        			   					    detailedFlowFileContent.append(separator);
	        			   					    
	        			   					    detailedFlowFileContent.append( subgroup.getFailed());
	        			   					    detailedFlowFileContent.append(separator);
	        			   					    
	        			   					    detailedFlowFileContent.append( subgroup.getLogicalOperatorSubGroupAsString());
	        			   					    detailedFlowFileContent.append(separator);
	        			   					    
	        			   					    detailedFlowFileContent.append(subgroup.getLogicalOperatorRulesAsString());
	        			   					    detailedFlowFileContent.append(separator);
	        			   					    
	        			   					    detailedFlowFileContent.append(rule.getId());
	        			   					    detailedFlowFileContent.append(separator);
	        			   				        
	        			   					    detailedFlowFileContent.append(rule.getFailed());
	        			   					    detailedFlowFileContent.append(separator);
	        			   					    
	        			   					    // append the resulting ruleengine message of the rule execution
	        			   					    detailedFlowFileContent.append(result.getMessage());
	        			   					    
	        			   					    // add line separator except last line
	        			   					    if(h<results.size()-1)
	        			   					    {
	        			   					    	detailedFlowFileContent.append(System.lineSeparator());
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
        }

        // for provenance reporting
        session.getProvenanceReporter().modifyAttributes(flowFile);
        
        // transfer the flow file
        session.transfer(flowFile, SUCCESS);

        // clear the collections of ruleengine results
    	ruleEngine.getRuleExecutionCollection().clear();
    }
    
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

    private void addRuleEngineProperties(Map<String, String> propertyMap, BusinessRulesEngine ruleEngine) throws Exception
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
