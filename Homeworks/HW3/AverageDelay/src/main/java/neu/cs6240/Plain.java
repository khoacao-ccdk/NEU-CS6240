package neu.cs6240;

import neu.cs6240.Utils.FlightPartitioner;
import neu.cs6240.Utils.FlightReducer;
import neu.cs6240.Utils.Mappers.FlightMapper;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static neu.cs6240.Utils.Configuration.NUM_REDUCER;

/**
 * This class performs the plain MapReduce approach to calculate the average delay time
 *
 * @author Cody Cao
 */
public class Plain {

  public static enum FlightCounters {
    TOTAL_FLIGHTS_MATCHED,
    TOTAL_MINUTES_DELAYED
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Average Delay - Plain");
    job.setJarByClass(Plain.class);
    job.setMapperClass(FlightMapper.class);

    job.setPartitionerClass(FlightPartitioner.class);
    //Setting number of reducer in accordance to the number of words
    job.setReducerClass(FlightReducer.class);
    job.setNumReduceTasks(NUM_REDUCER);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    Path outputPath = new Path(args[1]);
    FileOutputFormat.setOutputPath(job, outputPath);
    boolean success = job.waitForCompletion(true);

    if (success) {
      Counters counters = job.getCounters();

      long totalFlightsMatched = counters.findCounter(FlightCounters.TOTAL_FLIGHTS_MATCHED)
          .getValue();
      long totalMinutesDelayed = counters.findCounter(FlightCounters.TOTAL_MINUTES_DELAYED)
          .getValue();
      writeGlobalCounter(totalFlightsMatched, totalMinutesDelayed, outputPath, conf);
    }
    System.exit(success ? 0 : 1);
  }

  private static void writeGlobalCounter(long totalFlightsMatched, long totalMinutesDelayed,
      Path outputPath, Configuration conf) throws IOException {
    System.out.println("Output path:" + outputPath);
    Path outputFilePath = new Path(outputPath, "output.txt");

    // Create a writer to write to the output file
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream outputStream = fs.create(outputFilePath, true);

    // Write the counter values to the file
    double avgDelay = 1.0d * totalMinutesDelayed / totalFlightsMatched;
    outputStream.writeBytes("Total flights matched: " + totalFlightsMatched + "\n");
    outputStream.writeBytes("Total minutes delayed: " + totalMinutesDelayed + "\n");
    outputStream.writeBytes("Average delay minutes" + avgDelay);

    // Close the writer
    outputStream.close();
  }
}
