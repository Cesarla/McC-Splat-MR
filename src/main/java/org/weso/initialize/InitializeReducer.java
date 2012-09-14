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

	protected Set<String> followers = new HashSet<String>();

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
	protected void filterValues(Iterable<Text> values){
		followers.clear();
		for (Text value : values) {
			followers.add(value.toString());
		}
	}

	/**
	 * Write the result in the Hadoop Output.
	 * 
	 * @param key  User name of the current user.
	 * @param context The Context passed on to the Reducer implementations.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void writeResult(Text key, Context context) throws IOException,
			InterruptedException {
		for (String follower : followers) {
			context.write(key, new Text(follower));
		}
	}

}