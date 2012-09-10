package org.weso.finalize;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author César Luis Alvargonzález
 * 
 *         http://www.weso.es
 * 
 */
public class FinalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private final static String PROPERTY_INDICATOR = "#";
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] phrases = line.split("\t");
		Text userName = new Text(getUserName(phrases[0]));
		for(String property : getProperties(phrases[0])){
			context.write(new Text(userName), new Text(property));
		}
		
	}
	
	/**
	 * Loads current user property:value into a Map
	 * @return Map with user properties name and properties values.
	 */
	private Set<String> getProperties(String user){
		String chunks[] =  user.split(PROPERTY_INDICATOR);
		return new HashSet<String>(Arrays.asList(chunks));
	}
	
	/**
	 * Returns the user name of an user
	 * @param user User to find his user name 
	 * @return User name of an user
	 */
	private String getUserName(String user){
		String chunks[] =  user.split(PROPERTY_INDICATOR);
		return chunks[0];
	}
}
