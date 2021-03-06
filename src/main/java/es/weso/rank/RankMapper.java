package es.weso.rank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import es.weso.utils.Format;

/**
 * 
 * @author César Luis Alvargonzález
 * 
 *         http://www.weso.es
 * 
 */
public class RankMapper extends Mapper<LongWritable, Text, Text, Text> {

	protected final static String FOLLOWEE = "0";
	protected final static String FOLLOWER = "1";
	
	protected final static int VALID_LENGTH = 2;

	protected Context context;
	
	protected Text resultKey = new Text();
	protected Text resultValue = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] phrases = line.split("\t");

		this.context = context;

		String initialPhrase = phrases[0];
		
		String userName = getUserName(initialPhrase);
		if (isVerified(initialPhrase)) {
			writeFollowee(userName, initialPhrase);
		}
		
		for(String phrase: phrases){
			writeFollowee(phrase, initialPhrase);
			writeFollower(userName, phrase);
		}

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
	protected void writeFollower(String user, String follower)
			throws IOException, InterruptedException {
		StringBuilder out = new StringBuilder(FOLLOWER).append(follower);
		resultKey.set(user);
		resultValue.set(out.toString());
		context.write(resultKey, resultValue);
	}

	/**
	 * Write in the Hadoop Output an user name with a followee
	 * 
	 * @param user
	 *            Current user
	 * @param followee
	 *            User followed by the current user
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void writeFollowee(String user, String followee)
			throws IOException, InterruptedException {
		StringBuilder out = new StringBuilder(FOLLOWEE).append(followee);
		resultKey.set(user);
		resultValue.set(out.toString());
		context.write(resultKey, resultValue);
	}

	/**
	 * Extracts the user name for a phrase
	 * 
	 * @param phrase
	 *            Phrase to extract the user name
	 * @return User name of the phrase
	 */
	protected String getUserName(String phrase) {
        int end = phrase.indexOf(Format.PROPERTY_INDICATOR, 0);
        return phrase.substring(0, end);
	}

	/**
	 * Checks if a property is a Format.VERIFIED property
	 * 
	 * @param phrases
	 *            Array with the different user's properties
	 * @return true If the property is Format.VERIFIED
	 * @return false If the property is not Format.VERIFIED
	 */
	protected boolean isVerified(String phrase) {
		return phrase.indexOf(Format.VERIFIED) > -1;
	}

}