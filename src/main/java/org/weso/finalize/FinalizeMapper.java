package org.weso.finalize;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.weso.utils.Format;

/**
 * 
 * @author César Luis Alvargonzález
 * 
 *         http://www.weso.es
 * 
 */
public class FinalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text resultKey = new Text();
	private Text resultValue = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] phrases = line.split("\t");
		resultKey.set(getUserName(phrases[0]));
		for(String propertyRaw : getProperties(phrases[0])){
			resultValue.set(propertyRaw);
			context.write(resultKey, resultValue);
		}
	}
	
	/**
	 * Loads current user property:value into a Map
	 * @return Map with user properties name and properties values.
	 */
	private Set<String> getProperties(String user){
		String chunks[] =  user.split(Format.PROPERTY_INDICATOR);
		return new HashSet<String>(Arrays.asList(chunks));
	}
	
	/**
	 * Returns the user name of an user
	 * @param user User to find his user name 
	 * @return User name of an user
	 */
	private String getUserName(String user){
		String chunks[] =  user.split(Format.PROPERTY_INDICATOR);
		return chunks[0];
	}
}
