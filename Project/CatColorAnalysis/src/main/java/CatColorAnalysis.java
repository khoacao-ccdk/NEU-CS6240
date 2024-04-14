import Utils.CatColorKey;
import Utils.GroupingComparator;
import Utils.SortComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CatColorAnalysis {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Cat Color Analysis");
    job.setJarByClass(CatColorAnalysis.class);
    job.setMapperClass(CatColorMapper.class);
    job.setReducerClass(CatColorReducer.class);
    job.setMapOutputKeyClass(CatColorKey.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(CatColorKey.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setSortComparatorClass(SortComparator.class);
    job.setGroupingComparatorClass(GroupingComparator.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}
