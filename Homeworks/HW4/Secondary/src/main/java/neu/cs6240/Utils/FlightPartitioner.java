package neu.cs6240.Utils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlightPartitioner extends Partitioner<FlightKey, IntWritable> {

  @Override
  public int getPartition(FlightKey flightKey, IntWritable intWritable, int i) {
    return Math.abs(flightKey.hashCode()) % i;
  }
}
