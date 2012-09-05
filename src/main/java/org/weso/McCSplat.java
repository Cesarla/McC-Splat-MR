package org.weso;

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

import org.weso.finalize.FinalizeMapper;
import org.weso.finalize.FinalizeReducer;
import org.weso.initialize.InitializeMapper;
import org.weso.initialize.InitializeReducer;

import org.weso.rank.RankMapper;
import org.weso.rank.RankReducer;
import org.weso.utils.Mode;

/**
 * Analyzes the command line arguments, and starts the McCSplat Processing.
 * 
 * @author César Luis Alvargonzález
 * 
 *         http://www.weso.es
 * 
 */
public class McCSplat {

	private static String MCC_SPLAT_VERSION = "0.1.0.1207230";

	private CmdLineParser parser;

	@Argument(required = true, index = 0, usage = "File 'a' Follows 'b'", metaVar = "follows <File>")
	private String follows;

	@Argument(required = true, index = 1, usage = "Data", metaVar = "data <File>")
	private String data;

	@Argument(required = false, index = 2, usage = "mode <\"Plain Vanilla\"="
			+ Mode.PLAIN_VANILLA + ", \"Sink Absolute\"=" + Mode.SINK_ABSOLUTE
			+ ", \"Sink Relative\"=" + Mode.SINK_RELATIVE + ", \"Percentile\"="
			+ Mode.PERCENTILE, metaVar = "mode <Integer>")
	private Integer mode;

	@Option(name = "-h", aliases = { "--help" }, usage = "print this message")
	private boolean help = false;

	public static McCSplat MCCSPLAT_INSTANCE = null;

	private McCSplat() {
	}

	public static McCSplat getInstance() {
		if (MCCSPLAT_INSTANCE == null) {
			MCCSPLAT_INSTANCE = new McCSplat();
		}
		return MCCSPLAT_INSTANCE;
	}

	/**
	 * Set Command Line arguments
	 * 
	 * @param args
	 *            Command Line arguments
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

	private void validateInputData() throws IllegalArgumentException,
			IOException {
		validatePath(follows);
		validatePath(data);
		validateMode(mode);
	}

	private void validatePath(String path) throws IllegalArgumentException,
			IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		if (!fs.exists(new Path(path)))
			throw new IllegalArgumentException(path + " is no a valid path");
	}

	private void validateMode(Integer mode) throws IllegalArgumentException {
		if (mode != null && (mode < Mode.MIN_VALUE || mode > Mode.MAX_VALUE))
			throw new IllegalArgumentException(
					"Mode has to be an Integer between " + Mode.MIN_VALUE
							+ " and " + Mode.MIN_VALUE);
	}

	/**
	 * Analyzes the input arguments, and executes the job.
	 * 
	 * @throws Exception
	 */
	public void run() throws Exception {
		int i = 1;
		if (help == true) {
			displayHelp();
		} else {
			String pathName = "/user/hduser/out/_" + new Date().getTime();

			runInitializeJob(pathName);

			for (i = 1; i < 2; i++) {
				runRankJob(pathName, i);
			}

			//runFinalizeJob(pathName, i);
		}

	}

	private void runInitializeJob(String pathName) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("data", data);

		Job job = new Job(conf);
		job.setJobName("McCSPlat-Initialize");
		job.setJarByClass(this.getClass());
		job.setMapperClass(InitializeMapper.class);
		job.setReducerClass(InitializeReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(follows));
		FileOutputFormat.setOutputPath(job, new Path(pathName + "/1"));
		job.waitForCompletion(true);
	}

	private void runRankJob(String pathName, int i) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job;
		job = new Job();
		job.setJobName("McCSPlat-Rank");
		job.setJarByClass(this.getClass());
		job.setMapperClass(RankMapper.class);
		job.setReducerClass(RankReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(pathName + "/" + (i)));
		FileOutputFormat.setOutputPath(job, new Path(pathName + "/" + (i + 1)));
		job.waitForCompletion(true);
	}

	private void runFinalizeJob(String pathName, int i)
			throws CmdLineException, IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = null;
		Job job = null;
		conf = new Configuration();
		conf.setInt("mode", getMode());
		job = new Job(conf);
		job.setJobName("McCSPlat-Finalize");
		job.setJarByClass(this.getClass());
		job.setMapperClass(FinalizeMapper.class);
		job.setReducerClass(FinalizeReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(pathName + "/" + (i)));
		FileOutputFormat.setOutputPath(job, new Path(pathName + "/" + (i + 1)));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	private int getMode() throws CmdLineException {
		if (mode == null)
			return Mode.PLAIN_VANILLA;
		return mode;
	}

	/**
	 * Displays help information about hadoop-benchmark
	 */
	private void displayHelp() {
		System.out.println("McC-Splat-" + MCC_SPLAT_VERSION);
		parser.printUsage(System.out);
	}

	/**
	 * Initializes McC-Splat
	 * 
	 * @param args
	 *            Command Line arguments
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		McCSplat.getInstance().setArgs(args);
	}
}
