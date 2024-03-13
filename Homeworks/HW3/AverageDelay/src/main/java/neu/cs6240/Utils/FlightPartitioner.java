package neu.cs6240.Utils;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * This class partitions the flight data based on their flight date
 *
 * @author Cody Cao
 */
public class FlightPartitioner extends Partitioner<FlightKey, FlightValue> {

  @Override
  public int getPartition(FlightKey flightKey, FlightValue flightValue, int numPartitions) {
    return (flightKey.getFlightDate().hashCode() & Integer.MAX_VALUE) % numPartitions;
  }
}
