package org.weso.initialize;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class InitializeMapper extends Mapper<LongWritable, Text, Text, Text> {

	protected static String UNDEFINED = "UNDEF";
	protected static String NEW_DATA = "#100:";

	protected static Text SINK = new Text("sink");

	protected Map<String, String> verifiedData = null;

	protected Text resultKey = new Text();
	protected Text resultValue = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (this.verifiedData == null) {
			initializeMapper(context);
		}

		String line = value.toString();
		String[] phrases = value.toString().split("\t");

		if (line.isEmpty())
			return;

		resultKey.set(generateUser(phrases[0]));
		resultValue.set(phrases[1]);
		context.write(resultKey, resultValue);
		context.write(resultKey, SINK);

		resultKey.set(generateUser(phrases[1]));
		context.write(resultKey, SINK);
	}

	/**
	 * Initialize the mapper, loading verified data, and writing the sink within
	 * Hadoop out
	 * 
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void initializeMapper(Context context) throws IOException,
			InterruptedException {
		this.verifiedData = readVerifiedData(getVerifiedDataPath(context));

		resultKey.set(generateUser(SINK.toString()));
		resultValue.set("nobody");
		context.write(resultKey, resultValue);
	}

	/**
	 * Generates a user "chunk" for a specific user name.
	 * 
	 * @param userName
	 *            User name of the user "chunk"
	 * @return User "chunk" for a specific user name.
	 */
	protected String generateUser(String userName) {
		String type = getProperty(userName);
		StringBuilder user = new StringBuilder(userName).append(NEW_DATA)
				.append(type);
		if (!type.equals(UNDEFINED)) {
			user.append(Format.VERIFIED);
		}
		return user.toString();
	}

	/**
	 * Returns the property name assigned to the user name.
	 * 
	 * @param userName
	 *            User name who is searched for a value
	 * @return Property name assigned to the user name.
	 */
	protected String getProperty(String userName) {
		String property = this.verifiedData.get(userName);
		if (property == null) {
			property = UNDEFINED;
		}
		return property;
	}

	/**
	 * Returns the verified data file path set in the parameters.
	 * 
	 * @param context
	 *            The Context passed on to the mapper implementations.
	 * @return Verified data file path set in the parameters.
	 */
	protected String getVerifiedDataPath(Context context) {
		Configuration conf = context.getConfiguration();
		return conf.get("verifiedData");
	}

	/**
	 * Read the verified data file and load it into a Map.
	 * 
	 * @param path
	 *            Path of the defined data file.
	 * @return A map of user names as keys and verified properties as values
	 * @throws IOException
	 */
	protected Map<String, String> readVerifiedData(String path)
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
