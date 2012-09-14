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
	
	protected Text resultKey = new Text();
	protected Text resultValue = new Text();
	
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
	protected Set<String> getProperties(String user){
		String chunks[] =  user.split(Format.PROPERTY_INDICATOR);
		 Set<String> set = new HashSet<String>(Arrays.asList(chunks));
		 set.remove(getUserName(user));
		 return set;
	}
	
	/**
	 * Returns the user name of an user
	 * @param user User to find his user name 
	 * @return User name of an user
	 */
	protected String getUserName(String user){
		String chunks[] =  user.split(Format.PROPERTY_INDICATOR);
		return chunks[0];
	}
}
