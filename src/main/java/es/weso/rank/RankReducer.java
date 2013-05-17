package es.weso.rank;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import es.weso.utils.Format;

/**
 * 
 * @author César Luis Alvargonzález
 * 
 *         http://www.weso.es
 * 
 */
public class RankReducer extends Reducer<Text, Text, Text, Text> {

	protected final static char FOLLOWEE = '0';
	protected final static char FOLLOWER = '1';

	protected final static String UNDEFINED = "#100:UNDEF";

	protected Set<String> followees = new HashSet<String>();
	protected Set<String> followers = new HashSet<String>();

	protected int followeesSize = 0;
	protected String currentUser = null;
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
	 * Initialize the reducer, cleaning the followers and followees sets, and
	 * setting the current user.
	 * 
	 * @param key
	 *            Current User
	 */
	protected void resetReducer(Text key) {
		followees.clear();
		followers.clear();
		currentUser = key.toString();
	}

	/**
	 * Loads the followees and followers in its set.
	 * 
	 * @param values
	 *            Iterable of followers and followees of the current user.
	 */
	protected void loadUsers(Iterable<Text> values) {
		String user;
		for (Text value : values) {
			user = value.toString();
			switch (user.charAt(0)) {
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
	protected void calculateRank() {
		Map<String, Double> values = loadValues();
		StringBuilder out = new StringBuilder(currentUser);
		if (values.size() > 0) {
			for (Entry<String, Double> pair : values.entrySet()) {
				out.append(Format.PROPERTY_INDICATOR)
						.append((Double) pair.getValue() / followeesSize)
						.append(Format.PROPERTY_SEPARATOR)
						.append(pair.getKey());
			}
		} else {
			out.append(UNDEFINED);
		}

		resultKey.set(out.toString());
	}

	/**
	 * Write the results in the Hadoop Output.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void writeResults() throws IOException, InterruptedException {
		StringBuilder output = new StringBuilder();
		for (String follower : followers) {
			output.append(follower).append("\t");
		}
		resultValue.set(output.toString());
		context.write(resultKey, resultValue);
		if (currentUser.equals("sink")) {
			saveSinkData(resultKey.toString());
		}
	}

	/**
	 * Loads current user properties into a Map
	 * 
	 * @return Map with user properties name and properties values.
	 */
	protected Map<String, Double> loadValues() {
		Map<String, Double> values = new HashMap<String, Double>();
		for (String followee : followees) {
			for (Entry<String, Double> valuePair : getValues(followee)
					.entrySet()) {
				if (valuePair.getKey().contains(Format.VERIFIED)) {
					values.clear();
					values.put(valuePair.getKey(), valuePair.getValue());
					followeesSize = 1;
					return values;
				}
				updateValue(values, valuePair);
			}
		}
		return values;
	}

	private void updateValue(Map<String, Double> values,
			Entry<String, Double> valuePair) {
		Double value = values.get(valuePair.getKey());
		if (value == null) {
			value = 0d;
		}
		values.put(valuePair.getKey(), value + valuePair.getValue());
	}

	/**
	 * Loads current user property:value into a Map
	 * 
	 * @return Map with user properties name and properties values.
	 */
	protected Map<String, Double> getValues(String user) {

		Map<String, Double> map = new HashMap<String, Double>();
		String chunks[] = user.split(Format.PROPERTY_INDICATOR);
		String propertyPair[] = null;
		String name = null;
		for (String chunk : chunks) {
			propertyPair = chunk.split(Format.PROPERTY_SEPARATOR);
			if (propertyPair.length >= 2) {
				name = propertyPair[1];
				if (name.contains(Format.VERIFIED)) {
					if (currentUser.equals(getUserName(user))) {
						map.clear();
						map.put(name, new Double(propertyPair[0]));
						return map;
					}
					name = name.substring(0,
							name.length() - Format.VERIFIED.length());
				}
				map.put(name, new Double(propertyPair[0]));
			}
		}
		return map;
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

	/**
	 * Save current sink data in the file system, from later processing.
	 * 
	 * @param data
	 *            Data of the current sink node (user name with properties)
	 * @throws IOException
	 */
	protected void saveSinkData(String data) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		
		String executionPath = context.getConfiguration().get("executionPath");
		int iteration = context.getConfiguration().getInt("iteration",0);
		
		Path path = new Path(executionPath + "/sink/part-r-00000");
		Path oldPath = new Path(executionPath + "/sink/"+iteration+"/part-r-00000");
		
		if (fs.exists(oldPath) && !fs.delete(oldPath, false))
			throw new IOException("Error while deleting \"" + path.toString()
					+ "\"");
		writeData(data, fs, oldPath);
		if (fs.exists(path) && !fs.delete(path, false))
			throw new IOException("Error while deleting \"" + path.toString()
					+ "\"");
		writeData(data, fs, path);
	}


	private void writeData(String data, FileSystem fs, Path oldPath)
			throws IOException {
		FSDataOutputStream out = null;
		try {
			out = fs.create(oldPath);
			out.writeUTF(data);
		} finally {
			out.close();
		}
	}
}
