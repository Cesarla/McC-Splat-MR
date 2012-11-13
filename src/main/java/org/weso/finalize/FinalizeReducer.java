package org.weso.finalize;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.weso.utils.Format;
import org.weso.utils.Mode;

/**
 * 
 * @author César Luis Alvargonzález
 * 
 *         http://www.weso.es
 * 
 */
public class FinalizeReducer extends Reducer<Text, Text, Text, Text> {

	protected String currentPath = null;
	protected Context context = null;
	protected Map<String, Double> sinkProperties = null;
	protected int mode = Mode.PLAIN_VANILLA;
	protected int percentile = 0;

	protected Text resultValue = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		if (sinkProperties == null) {
			this.context = context;
			initializeReducer();
		}
		
		Map<String, Double> properties = processValues(values);
		
		switch (mode) {
			case Mode.PLAIN_VANILLA:
				processPlainVanilla(properties);
				break;
			case Mode.SINK_ABSOLUTE:
				processSinkAbsolute(properties);
				break;
			case Mode.SINK_RELATIVE:
				processSinkRelative(properties);
				break;
			case Mode.PERCENTILE:
				processPercentile(properties);
				break;
		}
		
		if (!resultValue.toString().equals(Format.UNDEFINED))
			context.write(key, resultValue);
	}

	/**
	 * Initialize the reducer
	 * 
	 * @throws IOException
	 */
	protected void initializeReducer() throws IOException {
		this.currentPath = context.getConfiguration().get("executionPath");
		this.mode = context.getConfiguration().getInt("mode",
				Mode.PLAIN_VANILLA);
		this.percentile = context.getConfiguration().getInt("percentile", 70);
		this.sinkProperties = loadSinkValues();
	}

	/**
	 * Returns a Map with property names as key and property values as value
	 * 
	 * @param values
	 *            Set of Text containing the property name mixed with the value
	 * @return Map with property names as key and property values as value
	 */
	protected Map<String, Double> processValues(Iterable<Text> values) {
		Map<String, Double> properties = new HashMap<String, Double>();
		String[] property = null;
		for (Text text : values) {
			property = text.toString().split(Format.PROPERTY_SEPARATOR);
			properties.put(property[1], new Double(property[0]));
		}
		return properties;
	}

	/**
	 * Read the Format.VERIFIED data file and load it into a Map.
	 * 
	 * @param path
	 *            Path of the defined data file.
	 * @return A map of user names as keys and Format.VERIFIED properties as
	 *         values
	 * @throws IOException
	 */
	protected Map<String, Double> loadSinkValues() throws IOException {
		Map<String, Double> sinkProperties = new HashMap<String, Double>();
		FileSystem fs = FileSystem.get(new Configuration());
		Path data = new Path(currentPath + "/sink/part-r-00000");
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(data)));
		try {
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] phrases = line.split(Format.PROPERTY_INDICATOR);
				for (int i = 1; i < phrases.length; i++) {
					String[] property = phrases[i]
							.split(Format.PROPERTY_SEPARATOR);
					sinkProperties.put(property[1], new Double(property[0]));
				}
			}
		} finally {
			br.close();
		}
		return sinkProperties;
	}

	/**
	 * Selects the property with the bigger value
	 * 
	 * @param properties
	 *            Map with property names as key and property values as value
	 */
	protected void processPlainVanilla(Map<String, Double> properties) {
		Entry<String, Double> biggerProperty = null;
		for(Entry<String,Double> current : properties.entrySet()){
			if (biggerProperty == null
					|| current.getValue() > biggerProperty.getValue())
				biggerProperty = current;
		}
		setResult(biggerProperty);

	}

	/**
	 * Selects the property with the bigger value and above the corresponding
	 * property in the sink node
	 * 
	 * @param properties
	 *            Map with property names as key and property values as value
	 */
	protected void processSinkAbsolute(Map<String, Double> properties) {
		Entry<String, Double> biggerProperty = null;
		for(Entry<String, Double> current : properties.entrySet()){
			if ((biggerProperty == null || current.getValue() > biggerProperty
					.getValue())
					&& current.getValue() > sinkProperties
							.get(getPropertyName(current.getKey())))
				biggerProperty = current;
		}
		setResult(biggerProperty);
	}

	/**
	 * Selects the property with the highest positive difference against the
	 * corresponding weight within the sink node
	 * 
	 * @param properties
	 *            Map with property names as key and property values as value
	 */
	protected void processSinkRelative(Map<String, Double> properties) {
		Entry<String, Double> biggerProperty = null;
		for(Entry<String,Double> current : properties.entrySet()){
			if (biggerProperty == null
					|| isPositiveDifference(current, biggerProperty))
				biggerProperty = current;
		}
		setResult(biggerProperty);
	}

	/**
	 * Selects the property with highest percentile –according to the labeled
	 * individuals
	 * 
	 * @param properties
	 *            Map with property names as key and property values as value
	 */
	protected void processPercentile(Map<String, Double> properties) {
		Entry<String, Double> biggerProperty = null;
		for (Entry<String, Double> current : properties.entrySet()) {
			if ((biggerProperty == null || current.getValue() > biggerProperty
					.getValue()) && current.getValue() > percentile) {
				biggerProperty = current;
			}
		}
		setResult(biggerProperty);
	}

	/**
	 * Sets the result, if bigger property is null, sets the result to
	 * undefined.
	 * 
	 * @param biggerProperty
	 *            Property with the bigger value for a determined mode
	 */
	protected void setResult(Entry<String, Double> biggerProperty) {
		if (biggerProperty == null)
			resultValue.set(Format.UNDEFINED);
		else
			resultValue.set(biggerProperty.getKey());
	}

	/**
	 * 
	 * @param property
	 * @return
	 */
	protected String getPropertyName(String property) {
		if (property.contains(Format.VERIFIED))
			return property.substring(0, property.length() - 2);
		return property;
	}

	/**
	 * Compares if the current property has bigger positive difference then the
	 * current one
	 * 
	 * @param biggerProperty
	 *            Entry with the bigger positive difference
	 * @param current
	 *            Entry with the actual property to compare
	 * @return true If the current property has bigger positive difference then
	 *         the current one
	 * @return false if the current property has not bigger positive difference
	 *         then the current one
	 */
	protected boolean isPositiveDifference(Entry<String, Double> current,
			Entry<String, Double> biggerProperty) {
		Double currentDifference = current.getValue()
				- sinkProperties.get(getPropertyName(current.getKey()));
		Double biggerDifference = biggerProperty.getValue()
				- sinkProperties.get(biggerProperty.getKey());
		if (currentDifference > 0 && currentDifference > biggerDifference)
			return true;
		return false;
	}

}