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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfWritables;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistribution;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistributionEntry;
import edu.umd.cloud9.util.pair.PairOfObjectInt;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static class MyMapper extends Mapper<LongWritable, Text, PairOfWritables<Text,IntWritable>, IntWritable> {
    private static final PairOfWritables<Text,IntWritable> KEY = new PairOfWritables<Text,IntWritable>();
    private static final IntWritable TF = new IntWritable();
    private static final Text WORD = new Text();
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
        KEY.set(WORD, new IntWritable((int)docno.get()));
        context.write(KEY, TF);
        // Output is (PairofWritables(Text, IntWritable), IntWritable)
      }
    }
  }

  // Partitioner guarantees all data for each word goes to the same reducer.
  private static class MyPartitioner extends Partitioner<WritableComparablePair, IntWritable> {
	@Override
	public int getPartition(WritableComparablePair key, IntWritable value, int numReduceTasks) {
		return key.getLeftElement().hashCode() % numReduceTasks;
	}
  }

  private static class KeyComparator extends WritableComparator {
	protected KeyComparator() {
	  super(WritableComparablePair.class, true);
    }
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      WritableComparablePair pair1 = (WritableComparablePair)w1;
      WritableComparablePair pair2 = (WritableComparablePair)w2;
      
	    return pair1.compareTo(pair2);
	    //return pair1.getLeftElement().toString().compareTo(pair2.getLeftElement().toString());
    }
  }

  private static class WritableComparablePair extends 
      PairOfWritables<Text,IntWritable>
    implements WritableComparable<PairOfWritables<Text,IntWritable>> {
    
  	@Override
  	public int compareTo(PairOfWritables<Text, IntWritable> w1) {
  	  //This will compare BOTH elements of the given pairs
  	  int compareValue = this.getLeftElement().toString().compareTo(w1.getLeftElement().toString());
  	  if (compareValue == 0) {
  	    compareValue = this.getRightElement().compareTo(w1.getRightElement());
  	  }
  	  return compareValue;
    }
  }


  private static class MyReducer extends
      Reducer<PairOfWritables<Text,IntWritable>, IntWritable, Text, PairOfWritables<IntWritable, BytesWritable>> {
      private static final Text CURRWORD = new Text();
      private static int flag = 0;
      private static final ByteArrayOutputStream bOut = new ByteArrayOutputStream();
      private static final DataOutputStream out = new DataOutputStream(bOut);
      private static int DF = 0;
      private static int CURRGAP = 0;
      private static int LAST = 0;
 
      // Solution buffers each postings list, but in compressed form (raw bytes)
      
    @Override
    public void reduce(PairOfWritables<Text,IntWritable> key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
            
      if (flag == 0) {
        flag = 1;
        CURRWORD.set(key.getLeftElement());
      }
      
      if (CURRWORD.equals(key.getLeftElement())) {
        for (IntWritable val : values) {
          DF += 1;
          CURRGAP = key.getRightElement().get() - LAST; 
          LAST = key.getRightElement().get();
          WritableUtils.writeVInt(out, CURRGAP);
          WritableUtils.writeVInt(out, val.get());
        }
      }
      else {

         /*if(CURRWORD.toString().equals("starcross'd")){
           LOG.info("starcross'd Posting Byte Stream size: " + bOut.size());
         }*/
        
        // if(lastDocno == 0) {
//          LOG.info(CURRWORD.toString() + " posting list size = " + bOut.size());
//          LOG.info(new String(bOut.toByteArray()));
        // }



        BytesWritable BW = new BytesWritable(bOut.toByteArray());
        context.write(CURRWORD, new PairOfWritables<IntWritable, BytesWritable>(new IntWritable(DF), BW));

        CURRWORD.set(key.getLeftElement());
        DF = 0;
        CURRGAP = 0;
        LAST = 0;
        bOut.reset();

        for (IntWritable val : values) {
          DF += 1;
          CURRGAP = key.getRightElement().get() - LAST; 
          LAST = key.getRightElement().get();
          WritableUtils.writeVInt(out, CURRGAP);
          WritableUtils.writeVInt(out, val.get());
        }
      }
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

    job.setMapOutputKeyClass(PairOfWritables.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairOfWritables.class);
     job.setOutputFormatClass(MapFileOutputFormat.class);
    //TODO change back to 
//    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);
    job.setSortComparatorClass(KeyComparator.class);

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
