package es.weso.finalize;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import es.weso.utils.Format;

/**
 * 
 * @author César Luis Alvargonzález
 * 
 *         http://www.weso.es
 * 
 */
public class FinalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

	protected Text resultKey = new Text();
	protected Text resultValue = new Text();
	String userName;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] phrases = line.split("\t");
		userName = getUserName(phrases[0]);
		resultKey.set(userName);
		for (String propertyRaw : getProperties(phrases[0])) {
			resultValue.set(propertyRaw);
			context.write(resultKey, resultValue);
		}
	}

	/**
	 * Loads current user property:value into a Map
	 * 
	 * @return Map with user properties name and properties values.
	 */
	protected String[] getProperties(String user) {
		String chunks[] = user.split(Format.PROPERTY_INDICATOR);
		List<String> list = Arrays.asList(chunks);
		list.toArray(chunks);
		return chunks;
	}

	/**
	 * Returns the user name of an user
	 * 
	 * @param phrase
	 *            User to find his user name
	 * @return User name of an user
	 */
	protected String getUserName(String phrase) {
		 int end = phrase.indexOf(Format.PROPERTY_INDICATOR, 0);
	     return phrase.substring(0, end);
	}
}
