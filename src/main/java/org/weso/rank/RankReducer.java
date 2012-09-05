package org.weso.rank;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
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
public class RankReducer extends Reducer<Text, Text, Text, Text>{
	
	private final static char FOLLOWEE = '0';
	private final static char FOLLOWER = '1';
	private final static String UNDEFINED = "#100.0000:UNDEFINED";
	
	private Set<String> followees = new HashSet<String>();
	private Set<String> followers = new HashSet<String>();
	
	private int followeesSize = 0;
	private Text result = null;
	private String currentUser = null;
	
	private Context context = null;
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
	private void resetReducer(Text key) {
		followees.clear();
		followers.clear();
		currentUser = key.toString();
	}

	/**
	 * Loads the followees and followers in its set.
	 * @param values Iterable of followers and followees of the current user.
	 */
	private void loadUsers(Iterable<Text> values) {
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
	private void calculateRank(){
		Map<String, Double> values = loadValues();
		Iterator<java.util.Map.Entry<String, Double>> it = values.entrySet().iterator();
		StringBuilder out = new StringBuilder(currentUser);
		
		if(values.size()>0){
			while (it.hasNext()) {
				java.util.Map.Entry<String, Double> pair = it.next();
				out.append("#").append((Double)pair.getValue()/followeesSize).append(":").append(pair.getKey());
			}
		}else{
			out.append(UNDEFINED);
		}
		 
		result = new Text(out.toString());
	}
	
	/**
	 * Write the results in the Hadoop Output.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void writeResults() throws IOException,
	InterruptedException {
		for(String follower : followers){
			context.write(result, new Text(follower));
		}
	}

	private Map<String, Double> loadValues() {
		Map<String, Double> values = new HashMap<String, Double>();
		Iterator<java.util.Map.Entry<String, Double>> it;
		for(String followee : followees){
			 it = getValues(followee).entrySet().iterator();
			 while (it.hasNext()) {
				 java.util.Map.Entry<String, Double> pairs = it.next();
				 if(pairs.getKey().contains("@V")){
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
	
	private String getName(String user){
		String chunks[] =  user.split("#");
		return chunks[0];
	}
	
	private Map<String,Double> getValues(String user){
		
		Map<String,Double> map = new HashMap<String, Double>();
		
		String chunks[] =  user.split("#");
		
		for(int i=0;i<chunks.length;i++){
			String aux[] =  chunks[i].split(":");
			if(aux.length>=2){
				String name = aux[1];
				if(name.contains("@V")){
					if(currentUser.equals(getName(user))){
						map.clear();
						map.put(name, new Double(aux[0]));
						return map;
					}else{
						name = name.substring(0,name.length()-2);
					}
				}
				map.put(name, new Double(aux[0]));
			}
		}
		return map;
	}
}
