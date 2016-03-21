package com.datamelt.nifi.processors;

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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * This Apache Nifi processor will allow to split the incoming content of a flowfile
 * into separate fields using a defined separator.
 * <p>
 * The values of the individual fields will be assigned to flowfile attributes. Each attribute
 * is named using the defined field prefix plus the positional number of the field.
 * <p>
 * A number format can optionally be specified to format the column number. The number format needs
 * to be according to the Java DecimalFormat class.
 * <p>
 * <p>
 * Example:
 * <p>
 * A flow file with following content:
 * <p>
 * Peterson, Jenny, New York, USA
 * <p>
 * When the field prefix is set to "column_" and the field number format is set to "000" the result will be 4 attributes:
 * <p>
 * column_000 = Peterson
 * column_001 = Jenny
 * column_002 = New York
 * column_003 = USA
 *
 * @author uwe geercken - last update 2016-03-19
 */

@SideEffectFree
@Tags({"CSV", "attributes", "split"})
@CapabilityDescription("Splits the content from a flowfile into individual columns. The resulting attribute contains field prefix plus the column positional "
        + "number and the value from the content as an attribute.")

public class SplitToAttribute extends AbstractProcessor {
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    // map used to store the attribute name and its value from the content of the flowfile
    private final Map<String, String> propertyMap = new HashMap<>();

    private static final String PROPERTY_ATTRIBUTE_PREFIX_NAME = "Attribute prefix";
    private static final String PROPERTY_ATTRIBUTE_PREFIX_DEFAULT = "column_";
    private static final String PROPERTY_ATTRIBUTE_PREFIX_ATTRIBUTE_NAME = "attribute.prefix";

    private static final String PROPERTY_FIELD_SEPERATOR_NAME = "Field separator";

    private static final String PROPERTY_FIELD_NUMBER_NUMBERFORMAT_NAME = "Field Number Format";
    private static final String PROPERTY_FIELD_NUMBER_NUMBERFORMAT_DEFAULT = "000";


    private static final String RELATIONSHIP_SUCESS_NAME = "success";

    public static final PropertyDescriptor ATTRIBUTE_PREFIX = new PropertyDescriptor.Builder()
            .name(PROPERTY_ATTRIBUTE_PREFIX_NAME)
            .required(true)
            .defaultValue(PROPERTY_ATTRIBUTE_PREFIX_DEFAULT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specify which String is used to prefix the attributes. If the prefix is e.g. \"column\" then the attributes will be "
                    + "named: \"column0\", \"column1\",\"column2\", etc.")
            .build();

    public static final PropertyDescriptor FIELD_SEPARATOR = new PropertyDescriptor.Builder()
            .name(PROPERTY_FIELD_SEPERATOR_NAME)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specify the field separator used to split the incomming flow file content.")
            .build();

    public static final PropertyDescriptor FIELD_NUMBER_NUMBERFORMAT = new PropertyDescriptor.Builder()
            .name(PROPERTY_FIELD_NUMBER_NUMBERFORMAT_NAME)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(PROPERTY_FIELD_NUMBER_NUMBERFORMAT_DEFAULT)
            .description("Specify the number format for the field number. E.g. \"000\" to get a three digit formatting.")
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name(RELATIONSHIP_SUCESS_NAME)
            .description("The flowfile content was successfully split into individual fields")
            .build();

    @Override
    public void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ATTRIBUTE_PREFIX);
        properties.add(FIELD_SEPARATOR);
        properties.add(FIELD_NUMBER_NUMBERFORMAT);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ProcessorLog logger = getLogger();

        // get selected number format for the field number
        String numberFormat = context.getProperty(FIELD_NUMBER_NUMBERFORMAT).getValue();

        // for formatting the number
        final DecimalFormat df;
        if (numberFormat != null && !numberFormat.trim().equals("")) {
            df = new DecimalFormat(context.getProperty(FIELD_NUMBER_NUMBERFORMAT).getValue());
        } else {
            df = new DecimalFormat();
        }

        // get the flowfile
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try {


                    // get the flowfile content
                    String row = IOUtils.toString(in);

                    // check that we have data
                    if (row != null && !row.trim().equals("")) {
                        //put the information which field prefix was used to the map
                        propertyMap.put(PROPERTY_ATTRIBUTE_PREFIX_ATTRIBUTE_NAME, context.getProperty(ATTRIBUTE_PREFIX).getValue());

                        // Split the row into separate fields using the FIELD_SEPARATOR property
                        String[] fields = row.split(context.getProperty(FIELD_SEPARATOR).getValue());

                        // loop over the fields
                        if (fields != null && fields.length > 0) {
                            for (int i = 0; i < fields.length; i++) {
                                propertyMap.put(context.getProperty(ATTRIBUTE_PREFIX).getValue() + df.format(i), fields[i]);
                            }
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    logger.error("Failed to split data into fields using seperator: [" + context.getProperty(FIELD_SEPARATOR).getValue() + "]");
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
