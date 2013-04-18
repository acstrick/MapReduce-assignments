import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ExtractHourlyCountsAll extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractHourlyCountsAll.class);

  // Mapper: emits (token, 1) for every word occurrence.
  private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Reuse objects to save overhead of object creation.
    private final static IntWritable ONE = new IntWritable(1);
    private final static Text TIME_CHUNK = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      String [] fields = line.split("\\s+");  //group all whitespaces as delimiter
      if (fields.length > 5 && fields[0].length() > 15) {  
        if ((fields[2].compareTo("Jan")==0 && Integer.parseInt(fields[3]) > 22) | 
            (fields[2].compareTo("Feb")==0 && Integer.parseInt(fields[3]) < 9) ) {
          TIME_CHUNK.set(fields[2]+" "+fields[3]+" "+fields[4]);//].substring(0, 2)); 
          context.write(TIME_CHUNK, ONE);
        }
      }

    }
  }

  // Reducer: sums up all the counts.
  private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // Reuse objects.
    private final static IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public ExtractHourlyCountsAll() {}

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ })
  public int run(String[] args) throws Exception {
    
    String inputPath = "/user/shared/tweets2011/tweets2011.txt";
    String outputPath = "/user/acstrick/out-all";
    
    LOG.info("Tool: " + ExtractHourlyCountsAll.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(ExtractHourlyCountsAll.class.getSimpleName());
    job.setJarByClass(ExtractHourlyCountsAll.class);

    //job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

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
    ToolRunner.run(new ExtractHourlyCountsAll(), args);
  }
}