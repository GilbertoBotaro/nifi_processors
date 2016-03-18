package com.datamelt.nifi.test;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

/**
 * This processor for Apache Nifi will allow to merge the incomming data from a flowfile
 * with an Apache Velocity template.
 * 
 * The Velocity template contains placeholders (e.g. $column0 - also alternatively in brackets: ${column0}).
 * The data will be merged with the template and the placeholders are replaced with the real data/values.
 * 
 * This is done by splitting the incomming data into separate fields using a given field separator. Then
 * each resulting field will be named using the given field prefix and a running number. If the field prefix
 * is e.g. "column" and there are 3 fields then these are named: column0, column1 and column2. In the Velocity
 * template use these column names as $column0, $column1 and $column2.
 * 
 * Example:
 * In the processor the field seperator is set to "," (comma). The field prefix is defined as "column".
 *  
 * A flow file with following data:
 * 
 *  Peterson, Jenny, New York, USA
 *  
 * A template file "names.vm" with follwoing format:
 * 
 * {
 * 		"name": "$column0",
 * 		"first": "$column1",
 * 		"city": "$column2",
 * 		"country": "$column3"
 * }
 * 
 * After the data is merged, the placeholders in the template file are replaced with the real data from the
 * flowfile. This is the result:
 * 
 * {
 * 		"name": "Peterson",
 * 		"first": "Jenny",
 * 		"city": "New York",
 * 		"country": "USA"
 * }
 *
 * Can be used for any textual data formats such as CSV, HTML, XML, Json, etc.
 *
 * @author uwe geercken - last update 2016-03-17
 */
@SideEffectFree
@Tags({"Template Engine", "Template", "Apache Velocity", "CSV", "format"})
@CapabilityDescription("Use an Apache Velocity template to convert data from a CSV file into a different format. Data is split into fields using the [Field separator] property. The resulting fields are available as [Field prefix] plus the running number of the field. E.g. if the [Field prefix] property is set to \"column\" then the fields are available as column0, column 1, column3, etc. In the Apache Velocity Template use then $column0, $column1, $column2, etc as placeholders. The data from the flowfile with be inserted into the template where the placeholders are.")

public class TemplateProcessor extends AbstractProcessor
{
   
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    // Apache Velocity Engine
    private VelocityEngine velocityEngine;
    // Apache Velocity Template
    private Template template; 
    
    public static final String MATCH_ATTR = "match";
    
    // tell velocity to load a template from a path
    private static final String RESOURCE_PATH = "file.resource.loader.path";
    
    public static final PropertyDescriptor TEMPLATE_PATH = new PropertyDescriptor.Builder()
            .name("Template path")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specify the path to the folder where the Apache Velocity template file is located.")
            .build();
    
    public static final PropertyDescriptor TEMPLATE_NAME = new PropertyDescriptor.Builder()
            .name("Template name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specify the name of the Apache Velocity template file - without the path information.")
            .build();
    
    public static final PropertyDescriptor FIELD_PREFIX = new PropertyDescriptor.Builder()
            .name("Field prefix")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specify which String is used to prefix the individual fields. If the prefix is e.g. \"column\" then the fields will be named: \"column0\", \"column1\",\"column2\", etc. These names must correspond to the placeholders used in the Apache Velocity template. In the template you would use \"$column0\", \"$column1\", \"$column2\", etc.")
            .build();
    
    public static final PropertyDescriptor FIELD_SEPARATOR = new PropertyDescriptor.Builder()
            .name("Field separator")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specify the field seperator of the incomming flow file. This is usually a comma or semicolon. The process will split the flowfile into fields using this separator")
            .build();
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The flowfile content was successfully merged with the template")
            .build();
    
    @OnScheduled
    public void initialize(final ProcessContext context)
    {
    	// Apache Velocity Template Engine
        velocityEngine = new VelocityEngine();
        
        // Properties for the Velocity Engine
        Properties velocityProperties = new Properties();
        velocityProperties.setProperty(RESOURCE_PATH,context.getProperty(TEMPLATE_PATH).getValue());

        // Init the engine
        velocityEngine.init(velocityProperties);
        
        // get the template from the given path
        template = velocityEngine.getTemplate(context.getProperty(TEMPLATE_NAME).getValue());
    }
    
    @OnStopped
    public void cleanup(final ProcessContext context)
    {
        velocityEngine = null;
        template = null;
    }
    
    @Override
    public void init(final ProcessorInitializationContext context)
    {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(TEMPLATE_PATH);
        properties.add(TEMPLATE_NAME);
        properties.add(FIELD_PREFIX);
        properties.add(FIELD_SEPARATOR);
        this.properties = Collections.unmodifiableList(properties);
        
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
        
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ProcessorLog log = this.getLogger();
        final AtomicReference<String> value = new AtomicReference<>();
        
        FlowFile flowfile = session.get();
        
        
        
        session.read(flowfile, new InputStreamCallback() 
        {
            @Override
            public void process(InputStream in) throws IOException 
            {
                try
                {
                    String row = IOUtils.toString(in);

                    /**
                     * Split the row into separate fields using the FIELD_SEPARATOR property
                     */
            		String[] fields = row.split(context.getProperty(FIELD_SEPARATOR).getValue());
            		
            		/**
            		 * to results of the merge with the template are written to this StringWriter
            		 */
            		StringWriter writer = new StringWriter();

            		/**
            		 * Create a context which will hold the variables
            		 */
            		VelocityContext velocityContext = new VelocityContext();
            		
            		/**
            		 * loop over the fields
            		 */
            		if(fields!=null && fields.length>0)
            		{
	            		for(int i=0;i<fields.length;i++)
	            		{
	            			/**
	            			 * put each field value in the velocity context. the name of the variable will be the
	            			 * defined field prefix plus a running number of the field.
	            			 */
	            			velocityContext.put(context.getProperty(FIELD_PREFIX).getValue() +i, fields[i].trim());
	            		}
            		}

            		/**
            		 * merge the template with the context (data/variables)
            		 */
            		template.merge(velocityContext,writer);

            		/**
            		 * set the value to the resulting string
            		 */
            		value.set(writer.toString());
            		
                }
                catch(Exception ex)
                {
                    ex.printStackTrace();
                    log.error("Failed to merge data with template: " + context.getProperty(TEMPLATE_NAME).getValue() + ", in path: " +context.getProperty(TEMPLATE_PATH).getValue() );
                }
            }
        });
        
        // Write the results to an attribute 
        final String results = value.get();
        
        if(results != null && !results.isEmpty())
        {
            flowfile = session.putAttribute(flowfile, "match", results.toString());
        }
        
        // To write the results back out to flow file 
        flowfile = session.write(flowfile, new OutputStreamCallback() 
        {

            @Override
            public void process(OutputStream out) throws IOException 
            {
                	out.write(value.get().getBytes());
            
            }
        });
        
        session.transfer(flowfile, SUCCESS);                                                                                                                                                                                                                                                                                                                                                                                                 
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
