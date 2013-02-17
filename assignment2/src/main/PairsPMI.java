 /*
 * @author Amanda Strickler
 * A MapReduce "pairs" style program to calculate the pointwise mutual
 * information of words in the bible + shakespeare corpus.
 */


import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
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
//import edu.umd.cloud9.io.array.ArrayListWritableComparable;

public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {
    private static final FloatWritable ONE = new FloatWritable(1);
    //private static final ArrayListWritableComparable allWords = new ArrayListWritableComparable();
    private static final PairOfStrings PAIR = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      String word = null;
      String word2 = null;
      ArrayList<String> allWords = new ArrayList<String>();
      String line = ((Text) value).toString();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word = itr.nextToken();
		if (allWords.contains(word)==false) {
		  allWords.add(word);
		}
     }
     //int mark = 1;
     for (int i = 0; i < allWords.size(); i++) {
		word = allWords.get(i).toString();
		for (int j = 0; j < allWords.size(); j++) {
	  	word2 = allWords.get(j).toString();
	  	// Emit (x, y, 1)
	  	PAIR.set(word, word2);
	  	context.write(PAIR, ONE);
	}
	//mark = mark + 1;
	// Emit (x, *, 1)
	PAIR.set(word, "*");
	context.write(PAIR, ONE);
      }

    }
  }

  private static class MyCombiner extends
      Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
    private static final FloatWritable SUM = new FloatWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  private static class MyReducer extends
      Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
    private static final FloatWritable VALUE = new FloatWritable();
    private float marginal = 0.0f;

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      float sum = 0.0f;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      if (key.getRightElement().equals("*")) {

        // Write to file P(x) = total lines containing x / total lines
		//VALUE.set(sum / 156215);
		VALUE.set(sum);
        context.write(key, VALUE);
      } else {
	// Write to file P(x, y) = total lines containing X,y / total lines
        //VALUE.set(sum / 156215);
        VALUE.set(sum);
        context.write(key, VALUE);
      }
    }
  }

  protected static class MyPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
    @Override
    public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

    /**
     * Creates an instance of this tool.
     */
    public PairsPMI() {}

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

      LOG.info("Tool: " + PairsPMI.class.getSimpleName());
      LOG.info(" - input path: " + inputPath);
      LOG.info(" - output path: " + outputPath);
      LOG.info(" - number of reducers: " + reduceTasks);

      Configuration conf = getConf();
      Job job = Job.getInstance(conf);
      job.setJobName(PairsPMI.class.getSimpleName());
      job.setJarByClass(PairsPMI.class);

      job.setNumReduceTasks(reduceTasks);

      FileInputFormat.setInputPaths(job, new Path(inputPath));
      FileOutputFormat.setOutputPath(job, new Path(outputPath));

      job.setMapOutputKeyClass(PairOfStrings.class);
      job.setMapOutputValueClass(FloatWritable.class);
      job.setOutputKeyClass(PairOfStrings.class);
      job.setOutputValueClass(FloatWritable.class);

      job.setMapperClass(MyMapper.class);
      job.setCombinerClass(MyReducer.class);
      job.setReducerClass(MyReducer.class);
      job.setPartitionerClass(MyPartitioner.class);

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
    ToolRunner.run(new PairsPMI(), args);
  }
}
