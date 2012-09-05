package org.weso.initialize;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class InitializeMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static String NEW_DATA = "#100.0000:";
	private static String VERIFIED = "@V";
	private static String UNDEFINED = "UNDEFINED";

	private static Text SINK = new Text("sink");
	private static Text NOBODY = new Text("nobody");

	private Pattern patternUndefined = Pattern
			.compile("^[a-zA-Z0-9_/.]{1,15}.*");

	private Map<String, String> verifiedData = null;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (this.verifiedData == null) {
			initializeMapper(context);
		}

		String line = value.toString();
		String[] phrases = line.split("\t");

		if (line.isEmpty())
			return;

		if (patternUndefined.matcher(line).find()) {
			Text user = generateUser(phrases[0]);
			context.write(user, new Text(phrases[1]));
			context.write(user, SINK);

			user = generateUser(phrases[1]);
			context.write(user, SINK);
		}else{
			throw new IOException("Bad Formed input files, key:" + key + " line:\""
					+ line + "\" " + phrases.length);
		}
	}

	/**
	 * Initialize the mapper, loading verified data, and writing the sink within Hadoop out
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void initializeMapper(Context context) throws IOException,
			InterruptedException {
		this.verifiedData = readVerifiedData(getVerifiedDataPath(context));
		Text user = generateUser(SINK.toString());
		context.write(user, NOBODY);
	}

	/**
	 * Generates a user "chunk" for a specific user name.
	 * @param userName User name of the user "chunk"
	 */
	private Text generateUser(String userName) {
		Text user = null;
		String type = getProperty(userName);
		if (!type.equals(UNDEFINED)) {
			user = new Text(userName + NEW_DATA + type + VERIFIED);
		} else {
			user = new Text(userName + NEW_DATA + type);
		}
		return user;
	}

	/**
	 * Returns the property name assigned to the user name.
	 * 
	 * @param userName User name who is searched for a value
	 * @return Property name assigned to the user name.
	 */
	private String getProperty(String userName) {
		String property = this.verifiedData.get(userName);
		if (property == null) {
			property = UNDEFINED;
		}
		return property;
	}

	/**
	 * Returns the verified data file path set in the parameters.
	 * 
	 * @param context The Context passed on to the mapper implementations.
	 * @return Verified data file path set in the parameters.
	 */
	private String getVerifiedDataPath(Context context) {
		Configuration conf = context.getConfiguration();
		String path = conf.get("data");
		return path;
	}

	/**
	 * Read the verified data file and load it into a Map.
	 * @param path Path of the defined data file.
	 * @return A map of user names as keys and verified properties as values
	 * @throws IOException
	 */
	private Map<String, String> readVerifiedData(String path)
			throws IOException {
		Map<String, String> verifiedData = new HashMap<String, String>();
		FileSystem fs = FileSystem.get(new Configuration());
		Path data = new Path(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(data)));
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] phrases = line.split("\t");
			if (phrases.length >= 2)
				verifiedData.put(phrases[0], phrases[1]);
		}
		return verifiedData;
	}
}
