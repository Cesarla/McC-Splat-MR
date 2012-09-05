package org.weso.rank;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RankReducer extends Reducer<Text, Text, Text, Text>{
	
	public final static char FOLLOWEE = '0';
	public final static char FOLLOWER = '1';
	
	private static String UNDEFINED = "#100.0000:UNDEFINED";
	
	private Set<String> followees = new HashSet<String>();
	private Set<String> followers = new HashSet<String>();
	
	private int followeesSize = 0;
	
	private Text result;
	
	private String currentUser;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String user = null;
		followees.clear();
		followers.clear();
		currentUser = key.toString();
		
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
		
		calculateRank(key.toString());
		
		for(String follower : followers){
			context.write(result, new Text(follower));
		}
	}
	
	private void calculateRank(String name){
		Iterator<java.util.Map.Entry<String, Double>> it;
		StringBuilder out;
		
		Map<String, Double> values = loadValues();
		
		result = null;
		
		it = values.entrySet().iterator();
		
		out = new StringBuilder(name);
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
