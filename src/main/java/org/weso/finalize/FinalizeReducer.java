package org.weso.finalize;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.weso.utils.Mode;

/**
 * 
 * @author César Luis Alvargonzález
 * 
 *         http://www.weso.es
 * 
 */
public class FinalizeReducer extends Reducer<Text,Text,Text,Text>{

	private String currentPath = null;
	private Context context = null;
	private Map<String, Double> sinkProperties = null;
	private int mode = Mode.PLAIN_VANILLA;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		if(currentPath==null){
			this.context = context;
			initializeReducer();
		}
		Map<String, Double> properties = processValues(values);
		Text result = null;
		switch(mode){
			case Mode.PLAIN_VANILLA:
				result = processPlainVanilla(properties);
				break;
			case Mode.SINK_ABSOLUTE:
				result = processSinkAbsolute(properties);
				break;
			case Mode.SINK_RELATIVE:
				result = processSinkRelative(properties);
				break;
			case Mode.PERCENTILE:
				result = processPercentile(properties);
				break;
		}
		context.write(key, result);
	}
	
	/**
	 * Initialize the reducer
	 * @throws IOException
	 */
	private void initializeReducer() throws IOException {
		this.currentPath = context.getConfiguration().get("executionPath");
		this.mode = context.getConfiguration().getInt("mode", Mode.PLAIN_VANILLA);
		loadSinkValues();
	}
	
	private Map<String, Double> processValues(Iterable<Text>values){
		Map<String, Double> properties = new HashMap<String, Double>();
		String[] property = null;
		for(Text text : values){
			property = text.toString().split(":");
			if(property.length>=2)
				properties.put(property[1], new Double(property[0]));
		}
		return properties;
	}
	
	/**
	 * Read the verified data file and load it into a Map.
	 * @param path Path of the defined data file.
	 * @return A map of user names as keys and verified properties as values
	 * @throws IOException
	 */
	private Map<String, Double> loadSinkValues()
			throws IOException {
		this.sinkProperties = new HashMap<String, Double>();
		FileSystem fs = FileSystem.get(new Configuration());
		Path data = new Path(currentPath+"/sink/part-r-00000");
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(data)));
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] phrases = line.split("#");
			for(int i = 1; i < phrases.length; i++){
				String[] property = phrases[i].split(":");
				sinkProperties.put(property[1], new Double(property[0]));
			}
		}
		return sinkProperties;
	}

	private Text processPlainVanilla(Map<String, Double> properties) {
		Iterator<Entry<String, Double>> it = properties.entrySet().iterator();
		Entry<String, Double> biggerProperty = null;
		while(it.hasNext()){
			Entry<String, Double> current = it.next();
			if(biggerProperty == null || current.getValue()>biggerProperty.getValue())
				biggerProperty = current;
		}
		return new Text(biggerProperty.getKey());

	}
	
	private Text processSinkAbsolute(Map<String, Double> properties) {
		// TODO Auto-generated method stub
		return null;
	}
	
	private Text processSinkRelative(Map<String, Double> properties) {
		// TODO Auto-generated method stub
		return null;
	}
	
	private Text processPercentile(Map<String, Double> properties) {
		// TODO Auto-generated method stub
		return null;
	}

}