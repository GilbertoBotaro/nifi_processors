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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
//import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;

import com.datamelt.datagenerator.DataCreator;

/**
 * This Apache Nifi processor will generate data based on word lists.
 * 
 * 
 * 
 * @author uwe geercken - last update 2016-03-20
 */

@SideEffectFree
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"generate", "attributes", "data"})
@CapabilityDescription("Splits the content from a flowfile into individual columns. The resulting attribute contains field prefix plus the column positional number and the value from the content as an attribute.")

public class GenerateData extends AbstractProcessor
{
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    
    private final AtomicReference<byte[]> data = new AtomicReference<>();
    
    // map used to store the attribute name and its value from the content of the flowfile
    private final Map<String, String> propertyMap = new HashMap<>();
    
    private static final String PROPERTY_CATEGORIES_PATH_NAME = "Categories path";
    
    private static final String PROPERTY_ROWLAYOUT_PATH_NAME = "Rowlayout file path and name";
    
    private static final String PROPERTY_LANGUAGE_FOLDER_NAME = "Language";
    
    private static final String RELATIONSHIP_SUCESS_NAME = "success";
    
    public static final PropertyDescriptor WORDLISTS_PATH = new PropertyDescriptor.Builder()
            .name(PROPERTY_CATEGORIES_PATH_NAME)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specify the path where the category files are located")
            .build();
    
    public static final PropertyDescriptor ROWLAYOUT_PATH = new PropertyDescriptor.Builder()
            .name(PROPERTY_ROWLAYOUT_PATH_NAME)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specify the path and name of the rowlayout.xml file")
            .build();
    
    public static final PropertyDescriptor LANGUAGE_FOLDER = new PropertyDescriptor.Builder()
            .name(PROPERTY_LANGUAGE_FOLDER_NAME)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specify which language folder to use. The language folder is a subfolder of the " + PROPERTY_CATEGORIES_PATH_NAME)
            .build();
    
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The number of rows to be generated for each flowfile")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name(RELATIONSHIP_SUCESS_NAME)
            .description("The flowfile content was successfully split into individual fields")
            .build();
    
    @Override
    public void init(final ProcessorInitializationContext context)
    {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(WORDLISTS_PATH);
        properties.add(ROWLAYOUT_PATH);
        properties.add(LANGUAGE_FOLDER);
        properties.add(BATCH_SIZE);

        this.properties = Collections.unmodifiableList(properties);
        
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException 
    {
    	//final ProcessorLog logger = getLogger();
    	
    	final String data = null;
    	
        DataCreator creator = new DataCreator();
        creator.setRowlayoutFile(context.getProperty(ROWLAYOUT_PATH).getValue());
        creator.setCategoryFilesFolder(context.getProperty(WORDLISTS_PATH).getValue());
        
        creator.setNumberOfOutputLines(10);
        
        
        for(int i = 0; i < context.getProperty(BATCH_SIZE).asInteger(); i++) 
        {
            FlowFile flowFile = session.create();
            if (data!=null && data.length() > 0) 
            {
                flowFile = session.write(flowFile, new OutputStreamCallback() 
                {
                    @Override
                    public void process(final OutputStream out) throws IOException 
                    {
                        out.write(data.getBytes());
                    }
                });
            }
            session.getProvenanceReporter().create(flowFile);
            session.transfer(flowFile, SUCCESS);
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
