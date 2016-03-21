package com.datamelt.nifi.processors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

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
 * This processor for Apache Nifi will allow to merge the attributes from a flowfile with an Apache Velocity template. The Velocity template
 * contains placeholders (e.g. $column0 - alternatively in brackets: ${column0}).
 * <p>
 * In the merge process the attributes of the flowfile will be merged with the template placeholders which are replaced with the attribute values.
 * <p>
 * See the Apache Velocity website at http://velocity.apache.org for details on the template engine.
 * <p>
 * A filter (regular expression) has to be specified, defining which attributes shall be considered for the template engine. The original file is
 * be routed to the "original" relation and the result of the merge process replaces the content of the flowfile and is routed to the "merged"
 * relationship.
 * <p>
 * <p>
 * Example:
 * <p>
 * A flow file with following attributes:
 * <p>
 * column0 = Peterson
 * column1 = Jenny
 * column2 = New York
 * column3 = USA
 * <p>
 * A template file "names.vm" with below format. Placeholders start with a dollar sign and are optionally in curly brackets:
 * <p>
 * {
 * "name": "$column0",
 * "first": "$column1",
 * "city": "$column2",
 * "country": "$column3"
 * }
 * <p>
 * After the attributes are merged with the template, the placeholders in the template are replaced with the values from the
 * flowfile attributes. This is the result:
 * <p>
 * {
 * "name": "Peterson",
 * "first": "Jenny",
 * "city": "New York",
 * "country": "USA"
 * }
 * <p>
 * Can be used for any textual data formats such as CSV, HTML, XML, Json, etc.
 *
 * @author uwe geercken - last update 2016-03-19
 */
@SideEffectFree
@Tags({"Template Engine", "Template", "Apache Velocity", "CSV", "format", "convert"})
@CapabilityDescription("Takes the attributes of a flowfile, merges them with an Apache Velocity template and replaces the content of the flowfile with the result. "
        + "Specifying the name of an attribute in the template - using following format: $<attribute name> (example: $column_001) - will replace this placeholder "
        + "with the actual value from the attribute.")

public class MergeTemplate extends AbstractProcessor {

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    // Apache Velocity Engine
    private VelocityEngine velocityEngine;
    // Apache Velocity Template
    private Template template;

    public static final String MATCH_ATTR = "match";

    private static final String PROPERTY_TEMPLATE_PATH_NAME = "Template path";
    private static final String PROPERTY_TEMPLATE_NAME_NAME = "Template name";

    private static final String PROPERTY_ATTRIBUTE_FILTER_DEFAULT = ".*";


    // tell velocity to load a template from a path
    private static final String RESOURCE_PATH = "file.resource.loader.path";

    public static final PropertyDescriptor TEMPLATE_PATH = new PropertyDescriptor.Builder()
            .name(PROPERTY_TEMPLATE_PATH_NAME)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specify the path to the folder where the Apache Velocity template file is located. Multiple path may be specified by "
                    + "dividing them with a comma.")
            .build();

    public static final PropertyDescriptor TEMPLATE_NAME = new PropertyDescriptor.Builder()
            .name(PROPERTY_TEMPLATE_NAME_NAME)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specify the name of the Apache Velocity template file - without the path information.")
            .build();

    public static final PropertyDescriptor ATTRIBUTE_FILTER = new PropertyDescriptor.Builder()
            .name("Attribute Filter")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(PROPERTY_ATTRIBUTE_FILTER_DEFAULT)
            .description("Specify a filter in the form of a regular expression for the attributes to include.")
            .build();

    public static final Relationship MERGED = new Relationship.Builder()
            .name("merged")
            .description("The result of the merge of attributes with the template will be routed to this destination")
            .build();

    public static final Relationship ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original input file will be routed to this destination")
            .build();

    @OnScheduled
    public void initialize(final ProcessContext context) {
        final ProcessorLog log = this.getLogger();

        try {
            // Apache Velocity Template Engine
            velocityEngine = new VelocityEngine();

            // Properties for the Velocity Engine
            Properties velocityProperties = new Properties();
            velocityProperties.setProperty(RESOURCE_PATH, context.getProperty(TEMPLATE_PATH).getValue());

            // Init the engine
            velocityEngine.init(velocityProperties);

            // get the template from the given path
            template = velocityEngine.getTemplate(context.getProperty(TEMPLATE_NAME).getValue());
        } catch (Exception ex) {
            ex.printStackTrace();
            log.error("Failed to initialize the Apache Velocity template engine for template: "
                    + context.getProperty(TEMPLATE_NAME).getValue() + ", in path: " + context.getProperty(TEMPLATE_PATH).getValue());
        }

    }

    @OnStopped
    public void cleanup(final ProcessContext context) {
        velocityEngine = null;
        template = null;
    }

    @Override
    public void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(TEMPLATE_PATH);
        properties.add(TEMPLATE_NAME);
        properties.add(ATTRIBUTE_FILTER);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(MERGED);
        relationships.add(ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ProcessorLog log = this.getLogger();

        final AtomicReference<String> value = new AtomicReference<>();

        FlowFile flowfile = session.get();

        final FlowFile original = session.clone(flowfile);

        final Map<String, String> attributes = flowfile.getAttributes();

        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try {
                    // to results of the merge with the template are written to this StringWriter
                    StringWriter writer = new StringWriter();

                    // Create a context which will hold the variables
                    VelocityContext velocityContext = new VelocityContext();

                    // filter the entries based on the given attribute filter
                    String attributesFilter = context.getProperty(ATTRIBUTE_FILTER).getValue();

                    // loop over the map of attributes
                    for (Map.Entry<String, String> entry : attributes.entrySet()) {
                        if (entry.getKey().matches(attributesFilter)) {
                            velocityContext.put(entry.getKey(), entry.getValue());
                        }
                    }

                    // merge the template with the context (data/variables)
                    template.merge(velocityContext, writer);

                    // set the value to the resulting string
                    value.set(writer.toString());

                } catch (Exception ex) {
                    ex.printStackTrace();
                    log.error("Failed to merge attributes with template: " + context.getProperty(TEMPLATE_NAME).getValue() + ", in path: " + context.getProperty(TEMPLATE_PATH).getValue());
                }
            }
        });

        // To write the results back out to flow file
        flowfile = session.write(flowfile, new OutputStreamCallback() {

            @Override
            public void process(OutputStream out) throws IOException {
                out.write(value.get().getBytes());
            }
        });

        // send the merged result here
        session.transfer(flowfile, MERGED);

        //send the original flowfile content here
        session.transfer(original, ORIGINAL);
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
