package neu.cs6240.Utils.Reducer;

import java.io.IOException;
import java.util.*;

import neu.cs6240.Utils.FlightKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import neu.cs6240.Secondary;

public class FlightReducer extends Reducer<FlightKey, IntWritable, Text, Text> {

  /**
   * At this stage, a list of key should be grouped by airline, ordered by month ascending, each has a
   * set of values with a list of delayed minutes
   *
   * @param key A FlightKey object that contains airline and month info
   * @param values A List of
   * @param context
   * @throws IOException
   * @throws InterruptedException
   */
  public void reduce(FlightKey key, Iterable<IntWritable> values,
      Context context
  ) throws IOException, InterruptedException {
    int currMonth = 1;
    long totalDelayMinutes = 0;
    long numRecordCounted = 0;
    StringBuilder sb = new StringBuilder();
    sb.append(key.getAirline().toString());

    for(IntWritable v : values) {
      //If a new month is seen, add the current month's average delay to the output String
      if(Integer.parseInt(key.getMonth()) != currMonth) {
        long avgDelayedMin = Math.round(1.0d * totalDelayMinutes / numRecordCounted);
        sb.append(", (")
            .append( currMonth)
            .append(",")
            .append(avgDelayedMin)
            .append(")");

        currMonth = Integer.parseInt(key.getMonth());
      }

      totalDelayMinutes += v.get();
      numRecordCounted++;
    }

    while(currMonth++ < 12) {
      sb.append(", (")
          .append( currMonth)
          .append(",")
          .append(0) //Nothing found for this month
          .append(")");
    }

    context.write(new Text(), new Text(sb.toString()));
  }
}
