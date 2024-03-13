import static Utils.Configuration.NUM_REDUCER;

import Utils.IntSumReducer;
import Utils.Mappers.TokenizerMapperPerMapTally;
import Utils.Mappers.TokenizerMapperPerTaskTally;
import Utils.WordPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountPerTaskTally {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCountPerTaskTally.class);
    job.setMapperClass(TokenizerMapperPerTaskTally.class);
    job.setPartitionerClass(WordPartitioner.class);
    //Setting number of reducer in accordance to the number of words
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(NUM_REDUCER);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}