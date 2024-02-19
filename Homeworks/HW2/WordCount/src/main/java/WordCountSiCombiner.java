import static Utils.Configuration.NUM_REDUCER;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Component classes
import Utils.Mappers.TokenizerMapper;
import Utils.IntSumReducer;
import Utils.WordPartitioner;

public class WordCountSiCombiner {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCountSiCombiner.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);

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