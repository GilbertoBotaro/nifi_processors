# nifi_processors

The .nar file available here contains two processors. To use them with Apache Nifi, drop the nar file in the lib folder and restart Nifi.

Be aware that this is my first attempt to write processors for Nifi, so they might not be production ready.

1)
The SplitToAttribute processor for Apache Nifi will allow to split the incoming content of a flowfile into separate fields using a defined separator.

The values of the individual fields will be assigned to flowfile attributes. Each attribute is named using the defined field prefix plus the positional number of the field.

A number format can optionally be specified to format the column number. The number format needs to be according to the Java DecimalFormat class.


Example:

A flow file with following content:

Peterson, Jenny, New York, USA

When the field prefix is set to "column_" and the field number format is set to "000" the result will be 4 attributes:

column_000 = Peterson
column_001 = Jenny
column_002 = New York
column_003 = USA

2)
The MergeTemplate processor for Apache Nifi will allow to merge the attributes from a flowfile with an Apache Velocity template. The Velocity template contains placeholders (e.g. $column0 - alternatively in brackets: ${column0}). 

In the merge process the attributes of the flowfile will be merged with the template and the placeholders are replaced with the attribute values.

See the Apache Velocity website at http://velocity.apache.org for details on the template engine.

A filter (regular expression) has to be specified, defining which attributes shall be considered for the template engine.

The original file will be routed to the "original" relation and the result of the merge process will replace the content of the flowfile and is routed to the "merged" relationship.


Example:
 
A flow file with following attributes:

column0 = Peterson
column1 = Jenny
column2 = New York
column3 = USA
 
A template file "names.vm" with below format. Placeholders start with a dollar sign and are optionally in curly brackets:

{
		"name": "$column0",
		"first": "$column1",
		"city": "$column2",
		"country": "$column3"
}

After the attributes are merged with the template, the placeholders in the template are replaced with the values from the
flowfile attributes. This is the result:

{
		"name": "Peterson",
		"first": "Jenny",
		"city": "New York",
		"country": "USA"
}

Can be used for any textual data formats such as CSV, HTML, XML, Json, etc.


author uwe geercken - last update 2016-03-19
