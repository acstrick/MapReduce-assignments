 /*
 * @author Amanda Strickler
 * A MapReduce "pairs" style program to calculate the pointwise mutual
 * information of words in the bible + shakespeare corpus.
 */

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;

import edu.umd.cloud9.io.pair.PairOfStrings;
import edu.umd.cloud9.io.map.HMapSIW;

public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

  private static class MyMapper extends Mapper<LongWritable, Text, Text, HMapSIW> {
    private static final Text KEY = new Text();
    private static final HMapSIW MAP = new HMapSIW();

    @Override
    public void map(LongWritable key, Text line, Context context)
        throws IOException, InterruptedException {

    String text = ((Text) line).toString();
	String[] words = text.split("\\s+");

    //int mark = 1;  // Used to account for repeat counts, i.e. (x,y) and (y,x)
    for (int i = 0; i < words.length; i++) {
		// Make map for words[i] w/ count values for all unique cooccuring words
		MAP.clear();
		for (int j = 0; j < words.length; j++) {
			if (MAP.containsKey(words[j]) == false) {
				MAP.increment(words[j]);
			}
		}
		// Add a value to the map for * to be used in the total count for x
		MAP.increment("*");
		//mark = mark + 1;
		KEY.set(words[i]);
		// Emit (x, Y) where Y is all y's
		context.write(KEY, MAP);
	}
  }
}

private static class MyCombiner extends
      Reducer<Text, HMapSIW, Text, HMapSIW> {

    @Override
    public void reduce(Text key, Iterable<HMapSIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapSIW> iter = values.iterator();
      HMapSIW map = new HMapSIW();
      while (iter.hasNext()) {
        map.plus(iter.next());
      }
      context.write(key, map);
    }
  }


  // Reducer: sums up all the counts.
  private static class MyReducer extends
        Reducer<Text, HMapSIW, Text, HMapSIW> {

      @Override
      public void reduce(Text key, Iterable<HMapSIW> values, Context context)
          throws IOException, InterruptedException {
        Iterator<HMapSIW> iter = values.iterator();
        HMapSIW map = new HMapSIW();
        while (iter.hasNext()) {
          map.plus(iter.next());
        }
        context.write(key, map);
      }
  }

  /**
   * Creates an instance of this tool.
   */
  public StripesPMI() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(StripesPMI.class.getSimpleName());
    job.setJarByClass(StripesPMI.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(HMapSIW.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(HMapSIW.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
