package neu.cs6240;

import neu.cs6240.Utils.FlightGroupComparator;
import neu.cs6240.Utils.FlightKeyComparator;
import neu.cs6240.Utils.FlightPartitioner;
import neu.cs6240.Utils.FlightReducer;
import neu.cs6240.Utils.Mapper.FlightMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * This class performs the plain MapReduce approach to calculate the average delay time
 *
 * @author Cody Cao
 */
public class Secondary {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Average Delay - SecondarySort");
    job.setJarByClass(Secondary.class);
    job.setMapperClass(FlightMapper.class);
    job.setReducerClass(FlightReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setPartitionerClass(FlightPartitioner.class);
    job.setSortComparatorClass(FlightKeyComparator.class);
    job.setGroupingComparatorClass(FlightGroupComparator.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    Path outputPath = new Path(args[1]);
    FileOutputFormat.setOutputPath(job, outputPath);
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}
