package org.weso.rank;

import java.io.IOException;

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
public class RankMapper extends Mapper<LongWritable, Text, Text, Text> {

	private final static String FOLLOWEE = "0";
	private final static String FOLLOWER = "1";
	private final static String VERIFIED = "@V";
	private final static int VALID_LENGTH = 2;

	private Context context;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] phrases = line.split("\t");

		this.context = context;

		if (hasValidLength(phrases)) {
			writeFollowee(phrases[1], phrases[0]);
			
			//ok
			writeFollower(getUserName(phrases[0]), phrases[1]);
			
			//ok
			if (isVerified(phrases[0])) {
				writeFollowee(getUserName(phrases[0]), phrases[0]);
			}

		}

	}

	/**
	 * Checks if the array has a valid length
	 * 
	 * @param phrases
	 *            Array to check
	 * @return true If the array has a valid length
	 * @return false If the array has a invalid length.
	 */
	private boolean hasValidLength(String[] phrases) {
		return phrases.length >= VALID_LENGTH;
	}

	/**
	 * Write in the Hadoop Output an user name with a follower
	 * 
	 * @param user
	 *            Current user
	 * @param follower
	 *            Follower of the current user
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void writeFollower(String user, String follower)
			throws IOException, InterruptedException {
		StringBuilder out = new StringBuilder(FOLLOWER).append(follower);
		context.write(new Text(user), new Text(out.toString()));
	}

	/**
	 * Write in the Hadoop Output an user name with a followe
	 * 
	 * @param user
	 *            Current user
	 * @param followee
	 *            User followed by the current user
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void writeFollowee(String user, String followee)
			throws IOException, InterruptedException {
		StringBuilder out = new StringBuilder(FOLLOWEE).append(followee);
		context.write(new Text(user), new Text(out.toString()));
	}

	/**
	 * Extracts the user name for a phrase
	 * 
	 * @param phrase
	 *            Phrase to extract the user name
	 * @return User name of the phrase
	 */
	private String getUserName(String phrase) {
		String chunks[] = phrase.split("#");
		return chunks[0];
	}

	/**
	 * Checks if a property is a verified property
	 * 
	 * @param phrases
	 *            Array with the different user's properties
	 * @return true If the property is verified
	 * @return false If the property is not verified
	 */
	private boolean isVerified(String phrase) {
		return phrase.substring(phrase.length() - 2).equals(VERIFIED);
	}

}
