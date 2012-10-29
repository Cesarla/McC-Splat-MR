package org.weso.rank;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.weso.utils.Format;

/**
 * 
 * @author César Luis Alvargonzález
 * 
 *         http://www.weso.es
 * 
 */
public class RankReducer extends Reducer<Text, Text, Text, Text>{
	
	protected final static char FOLLOWEE = '0';
	protected final static char FOLLOWER = '1';
	
	protected final static String UNDEFINED = "#100:UNDEF";
	
	protected Set<String> followees = new HashSet<String>();
	protected Set<String> followers = new HashSet<String>();
	
	protected int followeesSize = 0;
	protected String currentUser = null;
	protected String executionPath = null;
	protected Context context = null;
	
	protected Text resultKey = new Text();
	protected Text resultValue = new Text();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		this.context = context;
		resetReducer(key);
		loadUsers(values);
		calculateRank();
		writeResults();
	}

	/**
	 * Initialize the reducer, cleaning the followers and followees sets,
	 * and setting the current user.
	 * @param key Current User
	 */
	protected void resetReducer(Text key) {
		this.executionPath = context.getConfiguration().get("executionPath");
		followees.clear();
		followers.clear();
		currentUser = key.toString();
	}

	/**
	 * Loads the followees and followers in its set.
	 * @param values Iterable of followers and followees of the current user.
	 */
	protected void loadUsers(Iterable<Text> values) {
		String user;
		for (Text value : values) {
			user = value.toString();
			
			switch(user.charAt(0)){
				case FOLLOWEE:
					followees.add(user.substring(1));
				break;
				case FOLLOWER:
					followers.add(user.substring(1));
				break;
			}
		}
		followeesSize = followees.size();
	}
	
	/**
	 * Calculate the rank for the current user.
	 */
	protected void calculateRank(){
		Map<String, Double> values = loadValues();
		Iterator<java.util.Map.Entry<String, Double>> it = values.entrySet().iterator();
		StringBuilder out = new StringBuilder(currentUser);
		
		java.util.Map.Entry<String, Double> pair = null;
		
		if(values.size()>0){
			while (it.hasNext()) {
				pair = it.next();
				out.append(Format.PROPERTY_INDICATOR).append((Double)pair.getValue()/followeesSize).append(Format.PROPERTY_SEPARATOR).append(pair.getKey());
			}
		}else{
			out.append(UNDEFINED);
		}
		 
		resultKey.set(out.toString());
	}
	
	/**
	 * Write the results in the Hadoop Output.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void writeResults() throws IOException,
	InterruptedException {
		for(String follower : followers){
			resultValue.set(follower);
			context.write(resultKey, resultValue);
		}
		if(currentUser.equals("sink"))
			saveSinkData(resultKey.toString());
	}

	/**
	 * Loads current user properties into a Map
	 * @return Map with user properties name and properties values.
	 */
	protected Map<String, Double> loadValues() {
		Map<String, Double> values = new HashMap<String, Double>();
		Iterator<java.util.Map.Entry<String, Double>> it;
		for(String followee : followees){
			 it = getValues(followee).entrySet().iterator();
			 while (it.hasNext()) {
				 java.util.Map.Entry<String, Double> pairs = it.next();
				 if(pairs.getKey().contains(Format.VERIFIED)){
					 values.clear();
					 values.put(pairs.getKey(), pairs.getValue());
					 followeesSize = 1;
					 return values;
				 }
				 Double value = values.get(pairs.getKey());
				 if(value == null){
					 value = new Double(0);
				 }
				 values.put(pairs.getKey(),value + pairs.getValue());
			 }
		}
		return values;
	}
	
	/**
	 * Loads current user property:value into a Map
	 * @return Map with user properties name and properties values.
	 */
	protected Map<String,Double> getValues(String user){
		
		Map<String,Double> map = new HashMap<String, Double>();	
		String chunks[] =  user.split(Format.PROPERTY_INDICATOR);
		
		for(int i=0;i<chunks.length;i++){
			String aux[] =  chunks[i].split(Format.PROPERTY_SEPARATOR);
			if(aux.length>=2){
				String name = aux[1];
				if(name.contains(Format.VERIFIED)){
					if(currentUser.equals(getUserName(user))){
						map.clear();
						map.put(name, new Double(aux[0]));
						return map;
					}else{
						name = name.substring(0,name.length()-Format.VERIFIED.length());
					}
				}
				map.put(name, new Double(aux[0]));
			}
		}
		return map;
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
	
	/**
	 * Save current sink data in the file system, from later processing.
	 * @param data Data of the current sink node (user name with properties)
	 * @throws IOException
	 */
	protected void saveSinkData(String data) throws IOException{
		FileSystem fs = FileSystem.get(new Configuration());
		Path path = new Path(executionPath+"/sink/part-r-00000");
		FSDataOutputStream out = null;
		if(fs.exists(path) && !fs.delete(path, false))
				throw new IOException("Error while deleting \""+path.toString()+"\"");
		try{
			out = fs.create(path);
			out.writeUTF(data);
		}finally{
			out.close();
		}
	}
}
