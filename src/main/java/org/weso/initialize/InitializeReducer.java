package org.weso.initialize;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author César Luis Alvargonzález
 * 
 *         http://www.weso.es
 * 
 */
public class InitializeReducer extends Reducer<Text, Text, Text, Text> {

	private Set<String> followee = new HashSet<String>();
	private Text resultValue = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		filterValues(values);
		writeResult(key, context);
	}

	/**
	 * Load the values within a HashSet.
	 * 
	 * @param values Iterable with the properties of a user.
	 */
	private void filterValues(Iterable<Text> values) throws IllegalArgumentException{
		if(values == null)
			throw new IllegalArgumentException("Values could not be a null value");
		followee.clear();
		for (Text value : values) {
			followee.add(value.toString());
		}
	}

	/**
	 * Write the result in the Hadoop Output.
	 * 
	 * @param currentUserName  User name of the current user.
	 * @param context The Context passed on to the Reducer implementations.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void writeResult(Text currentUserName, Context context) throws IOException,
			InterruptedException {
		for (String followeeRaw : followee) {
			resultValue.set(followeeRaw);
			context.write(currentUserName, resultValue);
		}
	}

}