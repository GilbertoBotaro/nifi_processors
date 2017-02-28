package com.datamelt.nifi.processors;

import java.io.IOException;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

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
    
    private static final String PROPERTY_CATEGORIES_PATH_NAME = "Categories folder";
    private static final String PROPERTY_ROWLAYOUT_PATH_NAME = "Rowlayout XML file folder location and filename";
    private static final String RELATIONSHIP_SUCESS_NAME = "success";
    
    private static final String FIELD_SEPERATOR_PROPERTY_NAME = "Field separator";
    
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
    
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
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
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name(RELATIONSHIP_SUCESS_NAME)
            .description("The flowfile content was successfully generated")
            .build();
    
    @Override
    public void init(final ProcessorInitializationContext context)
    {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(WORDLISTS_PATH);
        properties.add(ROWLAYOUT_PATH);
        properties.add(BATCH_SIZE);
        properties.add(FIELD_SEPARATOR);

        this.properties = Collections.unmodifiableList(properties);
        
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }
    
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws Exception
    {
    	dataCreator = new DataCreator();
    	
    	getLogger().debug("parsing rowlayout file from: " + context.getProperty(ROWLAYOUT_PATH).getValue());
    	dataCreator.parseRowLayoutFile(context.getProperty(ROWLAYOUT_PATH).getValue());
    	getLogger().debug("setting categories files folder to: " + context.getProperty(WORDLISTS_PATH).getValue());
    	dataCreator.setCategoryFilesFolder(context.getProperty(WORDLISTS_PATH).getValue());
    	getLogger().debug("setting number of output lines to 1");
       	dataCreator.setNumberOfOutputLines(1);
       	getLogger().debug("setting field separator to: " + context.getProperty(FIELD_SEPARATOR).getValue());
       	dataCreator.setFieldSeparator(context.getProperty(FIELD_SEPARATOR).getValue());
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException 
    {
    	// get a logger instance
    	final ComponentLog logger = getLogger();
    	
    	// buffer to hold the content data
        StringBuffer content = new StringBuffer();

        try
        {
        	getLogger().debug("creating flow file");
        	FlowFile flowFile = session.create();
        	
        	getLogger().debug("generating rows: context.getProperty(BATCH_SIZE).asInteger()");
        	for(AtomicReference<Integer> i = new AtomicReference<Integer>(0); i.get() < context.getProperty(BATCH_SIZE).asInteger(); i.set(i.get()+1)) 
	        {
	        	content.append(dataCreator.generateRow() );
	        	if(i.get()<context.getProperty(BATCH_SIZE).asInteger()-1)
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
                    	out.write(content.toString().getBytes());
                    }
                });
            }
        	
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
