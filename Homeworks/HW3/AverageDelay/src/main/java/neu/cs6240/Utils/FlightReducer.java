package neu.cs6240.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import neu.cs6240.Plain;

public class FlightReducer extends Reducer<FlightKey, FlightValue, Text, DoubleWritable> {


  static Map<String, List<FlightValue>> legOneMap = new HashMap<>(), legTwoMap = new HashMap<>();

  public void reduce(FlightKey key, Iterable<FlightValue> values,
      Context context
  ) throws IOException, InterruptedException {
    String flightDate = key.getFlightDate();
    Map<String, List<FlightValue>> legMap =
        key.getLeg() == FlightKey.LEG_ONE ? legOneMap : legTwoMap;
    List<FlightValue> valueList = legMap.getOrDefault(flightDate, new ArrayList<>());
    for (FlightValue v : values) {
      valueList.add(v);
    }
    legMap.put(flightDate, valueList);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    //Performs a join, then update global counter value
    for (Map.Entry entry : legOneMap.entrySet()) {
      String date = (String) entry.getKey();
      List<FlightValue> legOneList = (List<FlightValue>) entry.getValue();

      List<FlightValue> legTwoList = legTwoMap.getOrDefault(date, new ArrayList<>());
      for (FlightValue legOneFlight : legOneList) {
        for (FlightValue legTwoFlight : legTwoList) {
          if (legOneFlight.getDest().equals(legTwoFlight.getOrigin()) &&
              (Long.parseLong(legOneFlight.getArrTime()) < Long.parseLong(
                  legTwoFlight.getDeptTime()))) {
            long totalMinDelay =
                legOneFlight.getArrDelayMinute() + legTwoFlight.getArrDelayMinute();
            String value = new StringBuilder(legOneFlight.toString()).append(legTwoFlight.toString()).toString();
            context.write(new Text(value), new DoubleWritable(totalMinDelay * 1.0d));
            context.getCounter(Plain.FlightCounters.TOTAL_MINUTES_DELAYED).increment(totalMinDelay);
            context.getCounter(Plain.FlightCounters.TOTAL_FLIGHTS_MATCHED).increment(1);
            break;
          }
        }
      }
    }
  }
}
