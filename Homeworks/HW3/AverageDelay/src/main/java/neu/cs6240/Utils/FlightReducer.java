package neu.cs6240.Utils;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import neu.cs6240.Plain;

public class FlightReducer extends Reducer<Text, FlightValue, Text, FlightValue> {
  public void reduce(Text key, Iterable<FlightValue> values,
      Context context
  ) throws IOException, InterruptedException {
    List<FlightValue> legOneList = new ArrayList<>(), legTwoList = new ArrayList<>();
    System.out.println(key);

    //Assuming that flights that occur on the same day and depart from / arrive at the same transit airport are grouped together under the same key
    //Deep clone is needed since the reduce function is reusing the v object to deserialize data instead of creating new object every time
    for (FlightValue v : values) {
      if(v.getOrigin().equals(Configuration.ORIGIN_AIRPORT))
        legOneList.add(v.clone()); //Deep cloning to extract values
      else
        legTwoList.add(v.clone()); //Deep cloning to extract values
    }

    long sumMinDelay = 0;
    long sumFlightMatched = 0;
    for (FlightValue legOneFlight : legOneList) {
      for (FlightValue legTwoFlight : legTwoList) {

        //If leg two's departure time is later than leg one's arrival time
        if ((Integer.parseInt(legOneFlight.getArrTime()) < Integer.parseInt(legTwoFlight.getDeptTime()))) {
          long totalMinDelay =
                  legOneFlight.getArrDelayMinute() + legTwoFlight.getArrDelayMinute();
          sumMinDelay += totalMinDelay;
          sumFlightMatched++;
        }
      }
    }
    context.getCounter(Plain.FlightCounters.TOTAL_MINUTES_DELAYED).increment(sumMinDelay);
    context.getCounter(Plain.FlightCounters.TOTAL_FLIGHTS_MATCHED).increment(sumFlightMatched);
  }
}
