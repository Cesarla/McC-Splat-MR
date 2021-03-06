package es.weso;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.CmdLineParser;

import es.weso.finalize.FinalizeMapper;
import es.weso.finalize.FinalizeReducer;
import es.weso.initialize.InitializeMapper;
import es.weso.initialize.InitializeReducer;

import es.weso.rank.RankMapper;
import es.weso.rank.RankReducer;
import es.weso.utils.Mode;

/**
 * Analyzes the command line arguments, bootstrapping the McCSplat Process.
 * McCSplat is a Singleton Class.
 * 
 * @author César Luis Alvargonzález
 * 
 *         http://www.weso.es
 * 
 */
public class McCSplat {
	
	@Argument(required = true, index = 0, usage = "File 'a' Follows 'b'", metaVar = "follows <File>")
	private String follows = null;

	@Argument(required = true, index = 1, usage = "Data", metaVar = "data <File>")
	private String data = null;

	@Argument(required = false, index = 2, usage = "mode <\"Plain Vanilla\"="
			+ Mode.PLAIN_VANILLA + ", \"Sink Absolute\"=" + Mode.SINK_ABSOLUTE
			+ ", \"Sink Relative\"=" + Mode.SINK_RELATIVE + ", \"Percentile\"="
			+ Mode.PERCENTILE, metaVar = "mode <Integer>")
	protected Integer mode = null;
	
	@Argument(required = false, index = 3, usage = "Percentile", metaVar = "percentile <Integer>")
	private Integer percentile = null;

	@Option(name = "-h", aliases = { "--help" }, usage = "print this message")
	private boolean help = false;

	private final static String MCC_SPLAT_VERSION = "0.2.0";

	private final static int NUM_ITERATIONS = 50;
	
	private final static int KEPT_OLD_ITERATIONS = 3;
	
	private CmdLineParser parser = null;
	
	public static McCSplat MCCSPLAT_INSTANCE = null;

	private McCSplat() {
	}

	/**
	 * Returns an instance of McCSplat Class
	 * @return Instance of McCSplat Class
	 */
	public static McCSplat getInstance() {
		if (MCCSPLAT_INSTANCE == null) {
			MCCSPLAT_INSTANCE = new McCSplat();
		}
		return MCCSPLAT_INSTANCE;
	}

	/**
	 * Set Command Line arguments
	 * 
	 * @param args Command Line arguments
	 * @return
	 * @throws Exception
	 */
	public void setArgs(String[] args) throws Exception {
		parser = new CmdLineParser(this);
		try {
			parser.parseArgument(args);
			validateInputData();
			run();
		} catch (CmdLineException e) {
			if (follows == null && data == null && help == true) {
				displayHelp();
			} else {
				System.out.println(e.getMessage());
				System.out
						.println("-h or --help for display help about hadoop-benchmark");
			}
		}

	}

	/**
	 * Validates the data provided as command line arguments
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void validateInputData() throws IllegalArgumentException,
			IOException {
		validatePath(follows);
		validatePath(data);
		validateMode(mode);
		validatePercentile(percentile);
	}

	/**
	 * Checks if a path is a valid path
	 * @param path Path of a directory or a file
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void validatePath(String path) throws IllegalArgumentException,
			IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		if (!fs.exists(new Path(path)))
			throw new IllegalArgumentException(path + " is no a valid path");
	}

	/**
	 * Checks if the mode provided is valid.
	 * @param mode Mode of the McCSplat Algorithm.
	 * @throws IllegalArgumentException
	 */
	private void validateMode(Integer mode) throws IllegalArgumentException {
		if (mode != null && (mode < Mode.MIN_VALUE || mode > Mode.MAX_VALUE))
			throw new IllegalArgumentException(
					"Mode has to be an Integer between " + Mode.MIN_VALUE
							+ " and " + Mode.MIN_VALUE);
	}

	/**
	 * Checks if the percentile provided is valid
	 * @param percentile Percentile to evaluate in the "Percentile Flavor"
	 * @throws IllegalArgumentException
	 */
	private void validatePercentile(Integer percentile) throws IllegalArgumentException {
		if(mode != null && mode == Mode.PERCENTILE){	
			if (percentile==null || percentile < 0 && percentile > 100)
				throw new IllegalArgumentException(
						"Percentile has to be an Integer between 0 to 100");
		}
	}
	
	
	/**
	 * Analyzes the input arguments, and executes the job
	 * 
	 * @throws Exception
	 */
	public void run() throws Exception {
		int i = 1;
		if (help == true) {
			displayHelp();
		} else {
			String executionPath = "/user/hduser/out/_" + new Date().getTime();

			runInitializeJob(executionPath);

			for (i = 1; i < NUM_ITERATIONS; i++) {
				runRankJob(executionPath, i);
				removeOldData(executionPath, i);
				
			}

			runFinalizeJob(executionPath, i);
		}

	}

	/**
	 * Runs the Initialize Job
	 * @param executionPath Path of the Hadoop Output
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private void runInitializeJob(String executionPath) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("verifiedData", data);

		Job job = new Job(conf);
		job.setJobName("McCSPlat-Initialize");
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(InitializeMapper.class);
		job.setReducerClass(InitializeReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Compress Map output
		conf.set("mapred.compress.map.output","true");
		conf.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
		// Compress MapReduce output
		conf.set("mapred.output.compress","true");
		conf.set("mapred.output.compression.type","block");
		conf.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
		
		FileInputFormat.addInputPath(job, new Path(follows));
		FileOutputFormat.setOutputPath(job, new Path(executionPath + "/1"));
		job.waitForCompletion(true);
	}

	/**
	 * Executes the Rank Job
	 * @param executionPath Path of the Hadoop Output
	 * @param iteration Counter of the job execution
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private void runRankJob(String executionPath, int iteration) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.setStrings("executionPath", executionPath);
		conf.setInt("iteration", iteration);
		Job job = new Job(conf);
		
		job.setJobName("McCSPlat-Rank");
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(RankMapper.class);
		job.setReducerClass(RankReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Compress Map output
		conf.set("mapred.compress.map.output","true");
		conf.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
		// Compress MapReduce output
		conf.set("mapred.output.compress","true");
		conf.set("mapred.output.compression.type","block");
		conf.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
		
		FileInputFormat.addInputPath(job, new Path(executionPath + File.separator + (iteration)));
		FileOutputFormat.setOutputPath(job, new Path(executionPath + File.separator + (iteration + 1)));
		job.waitForCompletion(true);
	}
	
	
	private void removeOldData(String executionPath, int iteration) throws IOException{
		FileSystem fs = FileSystem.get(new Configuration());
		Path path = new Path(executionPath+File.separator+(iteration-KEPT_OLD_ITERATIONS));
		if(fs.exists(path) && !fs.delete(path, true))
			throw new IOException("Error while deleting \""+path.toString()+"\"");
	}

	/**
	 * Runs the Finalize Job
	 * @param executionPath Path of the Hadoop Output
	 * @param iteration Counter of the job execution
	 * @throws CmdLineException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private void runFinalizeJob(String executionPath, int iteration)
			throws CmdLineException, IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = null;
		
		Job job = null;
		conf = new Configuration();
		conf.setInt("mode", getMode());
		conf.set("executionPath", executionPath);
		job = new Job(conf);
		
		job.setJobName("McCSPlat-Finalize");
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(FinalizeMapper.class);
		job.setReducerClass(FinalizeReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Compress Map output
		conf.set("mapred.compress.map.output","true");
		conf.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
		// Compress MapReduce output
		conf.set("mapred.output.compress","true");
		conf.set("mapred.output.compression.type","block");
		conf.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
		
		FileInputFormat.addInputPath(job, new Path(executionPath + File.separator + (iteration)));
		FileOutputFormat.setOutputPath(job, new Path(executionPath + File.separator + (iteration + 1)));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * Returns the McCSplat mode
	 * @return McCSplat mode
	 * @throws CmdLineException
	 */
	protected int getMode() throws CmdLineException {
		if (mode == null)
			return Mode.PLAIN_VANILLA;
		return mode;
	}

	/**
	 * Displays the help information about McCSplat
	 */
	private void displayHelp() {
		System.out.println("McC-Splat-" + MCC_SPLAT_VERSION);
		parser.printUsage(System.out);
	}

	/**
	 * Initializes McC-Splat
	 * 
	 * @param args Command Line arguments
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		McCSplat.getInstance().setArgs(args);
	}
}