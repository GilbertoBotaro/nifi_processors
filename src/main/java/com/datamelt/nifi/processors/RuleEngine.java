package com.datamelt.nifi.processors;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipFile;

import org.apache.commons.io.IOUtils;
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
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.datamelt.rules.engine.BusinessRulesEngine;
import com.datamelt.util.Row;
import com.datamelt.util.RowFieldCollection;
import com.datamelt.util.Splitter;

/**
 * This Apache Nifi processor will allow to run the business rules engine JaRE against
 * an incoming flowfile. The results of the ruleengine will be stored in the flowfile
 * attributes and may then in turn be used for further decisions in the Nifi flow.
 * <p>
 * The ruleengine uses a project zip file, which was created in the Business Rules Maintenance Tool.
 * This is a web application to construct and orchestrate the business rules logic. The
 * tool allows to export the logic of a project into a single zip file.
 * <p>
 * The content of the flowfile is expected to be one row of comma separated data. The row
 * is split into its individual fields using the given field separator.
 * <p>

 * @author uwe geercken - last update 2017-02-24
 */

@SideEffectFree
@Tags({"CSV", "attributes", "ruleengine", "decision", "logic", "business rules"})
@CapabilityDescription("Runs a ruleengine project zip file containing business logic against the flow file content using the Business Rules Engine JaRE."
        + "The flowfile content is expected to be a single row of data in CSV format. This row of data is split into it's individual fields"
		+ "and then the business logic from the project zip file is applied to the fields."
        + "The project zip file is created in the Business Rules Maintenance Tool - a web application to construct and orchestrate business logic."
		+ "Because the business logic is separated from the Nifi flow and processors, when the business logic changes, the Nifi flow does not have to be changed."
        + "Instead the business logic is updated in the Business Rules maintenance tool and a new project zip file is created. This can be automated."
		)

public class RuleEngine extends AbstractProcessor {
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    // the business rules engine to execute business logic against data
    BusinessRulesEngine ruleEngine = null;
    
    // map used to store the attribute name and its value from the content of the flowfile
    private final Map<String, String> propertyMap = new HashMap<>();
    
    //these fields from the results of the ruleengine will be added to the flowfile attributes
    private static final String PROPERTY_RULEENGINE_ZIPFILE_NAME 				= "ruleengine.zipfile";
    private static final String PROPERTY_RULEENGINE_RULEGROUPS_COUNT			= "ruleengine.rulegroupsCount";
    private static final String PROPERTY_RULEENGINE_RULEGROUPS_PASSED 			= "ruleengine.rulegroupsPassed";
    private static final String PROPERTY_RULEENGINE_RULEGROUPS_FAILED 			= "ruleengine.rulegroupsFailed";
    private static final String PROPERTY_RULEENGINE_RULEGROUPS_SKIPPED 			= "ruleengine.rulegroupsSkipped";
    private static final String PROPERTY_RULEENGINE_RULES_COUNT 				= "ruleengine.rulesCount";
    private static final String PROPERTY_RULEENGINE_RULES_PASSED 				= "ruleengine.rulesPassed";
    private static final String PROPERTY_RULEENGINE_RULES_FAILED	 			= "ruleengine.rulesFailed";
    private static final String PROPERTY_RULEENGINE_ACTIONS_COUNT 				= "ruleengine.actionsCount";
    
    // names/labels of the processor attibutes
    private static final String RULEENGINE_ZIPFILE_PROPERTY_NAME = "Ruleengine Project Zip File";
    private static final String FIELD_SEPERATOR_PROPERTY_NAME = "Field separator";
    
    private static final String RELATIONSHIP_SUCESS_NAME = "success";

    public static final PropertyDescriptor ATTRIBUTE_RULEENGINE_ZIPFILE = new PropertyDescriptor.Builder()
            .name(RULEENGINE_ZIPFILE_PROPERTY_NAME)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specify the path and filename of the ruleengine project file to use")
            .build();

    public static final PropertyDescriptor ATTRIBUTE_FIELD_SEPARATOR = new PropertyDescriptor.Builder()
            .name(FIELD_SEPERATOR_PROPERTY_NAME)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specify the field separator to be used to split the incomming flow file content - a single row of CSV data.")
            .build();
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name(RELATIONSHIP_SUCESS_NAME)
            .description("The flowfile content was successfully split into individual fields and the ruleengine executed the business logic against the data (fields).")
            .build();

    @Override
    public void init(final ProcessorInitializationContext context) 
    {
        
    	List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ATTRIBUTE_RULEENGINE_ZIPFILE);
        properties.add(ATTRIBUTE_FIELD_SEPARATOR);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }
    
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws Exception
    {
    	// get the name of the ruleengine project zip file
    	String ruleEngineProjectFileName = context.getProperty(ATTRIBUTE_RULEENGINE_ZIPFILE).getValue();
    	
    	// put the name of the ruleengine zip file in the list of properties
        propertyMap.put(PROPERTY_RULEENGINE_ZIPFILE_NAME, ruleEngineProjectFileName );
        
        // 
        File file = new File(ruleEngineProjectFileName);
        ZipFile ruleEngineProjectFile = new ZipFile(file);
        getLogger().debug("ope");
        
        getLogger().info("initialized business rule engine version: " + BusinessRulesEngine.getVersion() + " using " + ruleEngineProjectFileName );
        getLogger().debug("field separator to split row into fields: " + context.getProperty(ATTRIBUTE_FIELD_SEPARATOR).getValue());
        
        ruleEngine = new BusinessRulesEngine(ruleEngineProjectFile);
        getLogger().debug("number of rulegroups in project zip file: " + ruleEngine.getNumberOfGroups());

        ruleEngine.setPreserveRuleExcecutionResults(false);

    }
    
    @OnUnscheduled
    public void onUnScheduled(final ProcessContext context) throws Exception
    {
        ruleEngine = null;
        getLogger().debug("processor unscheduled - set ruleengine object to null");
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException 
    {
    	// get a logger instance
    	final ComponentLog logger = getLogger();
    	
    	// clear the collections of ruleengine results
    	ruleEngine.getRuleExecutionCollection().clear();
    	
        // get the flowfile
        FlowFile flowFile = session.get();
        if (flowFile == null) 
        {
            return;
        }

        // read flowfile into inputstream
        session.read(flowFile, new InputStreamCallback() 
        {
            public void process(InputStream in) throws IOException 
            {
                try 
                {
                    // get the flowfile content
                    String row = IOUtils.toString(in);
                    logger.debug("read flowfile content" + row);
                    // check that we have data
                    if (row != null && !row.trim().equals("")) {
                        
                        // use the Splitter class to split the incomming row into fields
                    	// pass info which separator is used
                		Splitter splitter = new Splitter(Splitter.TYPE_COMMA_SEPERATED,context.getProperty(ATTRIBUTE_FIELD_SEPARATOR).getValue());
                		logger.debug("created Splitter object");

                		// convert the fields of the CSV file into a collection
                		// the given field separator is used to split the incoming data into fields
        		        RowFieldCollection collection = new RowFieldCollection(splitter.getFields(row));
        		        logger.debug("created RowFieldCollection object: " + collection.getNumberOfFields() + " number of fields");

        		        logger.debug("running business ruleengine...");
        		        
        		        // run the ruleengine with the given data
        		        ruleEngine.run("row ", collection);
        		        
        		        logger.debug("number of rulegroups: " + ruleEngine.getNumberOfGroups());
        		        logger.debug("number of rulegroups passed: " + ruleEngine.getNumberOfGroupsPassed());
        		        logger.debug("number of rulegroups failed: " + ruleEngine.getNumberOfGroupsFailed());
        		        logger.debug("number of rulegroups skipped: " + ruleEngine.getNumberOfGroupsSkipped());
        		        logger.debug("number of rules: " + ruleEngine.getNumberOfRules());
        		        logger.debug("number of rules passed: " + ruleEngine.getNumberOfRulesPassed());
        		        logger.debug("number of rules failed: " + ruleEngine.getNumberOfRulesFailed());
        		        logger.debug("number of actions: " + ruleEngine.getNumberOfActions());
        		        
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
        		        
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    logger.error("error running the business ruleengine",ex);
                }
            }
        });

        // put the map to the flowfile
        flowFile = session.putAllAttributes(flowFile, propertyMap);
        
        // for provenance
        session.getProvenanceReporter().modifyAttributes(flowFile);
        
        // transfer the flowfile
        session.transfer(flowFile, SUCCESS);
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
