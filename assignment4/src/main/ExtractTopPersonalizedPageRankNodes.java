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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.TopNScoredObjects;
import edu.umd.cloud9.util.pair.PairOfObjectFloat;

public class ExtractTopPersonalizedPageRankNodes {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);
  private static final String INPUT = "input";
  private static final String TOP = "top";
  private static final String SOURCE_NODES = "sources";

  @SuppressWarnings({ "static-access" })
  public static void main(String[] args) {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("top").hasArg()
        .withDescription("number of top nodes to print").create(TOP));
    options.addOption(OptionBuilder.withArgName("src").hasArg()
        .withDescription("comma-separated list of source nodes").create(SOURCE_NODES));


    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(TOP) || 
        !cmdline.hasOption(SOURCE_NODES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(ExtractTopPersonalizedPageRankNodes.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String src = cmdline.getOptionValue(SOURCE_NODES); 
    
    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - top: " + n);
    LOG.info(" - sources: " + src);
    
    String [] sources = src.split(",\\s*");  //TODO: change src to SOURCE_NODES
    String source = sources[0];
    String sourceIntro = "Source:";
    System.out.println(String.format("%.7s %s", sourceIntro, source));
    //Multiple-source TODO: output in the order of the args in the -sorted option
    
    
    TopNScoredObjects<Integer> queue = new TopNScoredObjects<Integer>(n);
    
    try {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      FileStatus[] fss = fs.globStatus(new Path(inputPath + "/part*"));
      for (FileStatus status : fss) {
        Path path = status.getPath();
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        IntWritable nodeid = new IntWritable();
        PageRankNode node = new PageRankNode();
      
        while (reader.next(nodeid, node)) {
          //The PageRank value is stored in log form; must convert it to normal form.
          queue.add(nodeid.get(), (float) Math.exp(node.getPageRank()));
        }
        reader.close();
      }
      
      for (PairOfObjectFloat<Integer> outPair : queue.extractAll()) {
        System.out.println(String.format("%.5f %d", 
            outPair.getRightElement(),outPair.getLeftElement()));
      }
      
    } catch (IOException e) {
      System.err.println("Error reading file: "+ e.getMessage());
      System.exit(-1);
    }
  
    
  }
}
