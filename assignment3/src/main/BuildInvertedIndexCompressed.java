/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfWritables;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistribution;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistributionEntry;
import edu.umd.cloud9.util.pair.PairOfObjectInt;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndex.class);

  private static class MyMapper extends Mapper<LongWritable, Text, PairOfWritables<Text,IntWritable>, IntWritable> {
    private static final PairOfWritables<Text,IntWritable> KEY = new PairOfWritables<Text,IntWritable>();
    private static final IntWritable TF = new IntWritable();
    private static final Text WORD = new Text():
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<String>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      String text = doc.toString();
      COUNTS.clear();

      String[] terms = text.split("\\s+");

      // First build a histogram of the terms.
      for (String term : terms) {
        if (term == null || term.length() == 0) {
          continue;
        }

        COUNTS.increment(term);
      }

      // Emit postings as ((word,docno), tf)
      for (PairOfObjectInt<String> e : COUNTS) {
		TF.set(e.getRightElement());
        WORD.set(e.getLeftElement());
        KEY.set(PairOfWritables(WORD, (int)docno.get());
        context.write(KEY, TF);
      }
    }
  }

  private static class MyPartitioner extends Partitioner<MyWritableComparable, IntWritable> {
	@Override
	public int getPartition(MyWritableComparable key, IntWritable value, int numReduceTasks) {
		return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
  }

  private static class KeyComparator extends WritableComparator {
	protected KeyComparator() {
	  super(MyWritableComparator,class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
	  MyWritableComparable pair1 = (MyWritableComparable) w1;
	  MyWritableComparable pair2 = (MyWritableComparable) w2;

	  int check = k1.compareTo(k2);
    }
  }

  private static class WritableComparablePair extends PairofWritables<Text,IntWritable>
    implements WritableComparable<MyWritableComparable> {
	@Override
	public int compareTo(MyWritableComparable w) {
	  //This will compare BOTH elements of the given pairs
	  int thisValue = this.value;
	  int thatValue = ((IntWritable)o).value;
	  return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
  }

   /*
    private static class GroupComparator extends WritableComparator {
  	protected GroupComparator() {
  	  super(IntPair.class, true);
      }
      //public int compare...
    }
  */

  private static class MyReducer extends
      Reducer<Text, PairOfInts, Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>> {
    private final static IntWritable DF = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<PairOfInts> values, Context context)
        throws IOException, InterruptedException {
      Iterator<PairOfInts> iter = values.iterator();
      ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();

      int df = 0;
      while (iter.hasNext()) {
        postings.add(iter.next().clone());
        df++;
      }

      // Sort the postings by docno ascending.
      Collections.sort(postings);

      DF.set(df);
      context.write(key,
          new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(DF, postings));
    }
  }

  private BuildInvertedIndexCompressed() {}

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

    LOG.info("Tool name: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(PairofWritables.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairOfWritables.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
