package neu.cs6240.Utils;

import neu.cs6240.Utils.Configuration;
import neu.cs6240.Utils.FlightHeader;

import neu.cs6240.Utils.FlightValue;
import com.opencsv.CSVParser;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper extends Mapper<Object, Text, Text, FlightValue> {
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    CSVParser parser = new CSVParser();
    String[] tokens = parser.parseLine(value.toString());
    String origin = tokens[FlightHeader.ORIGIN];
    String dest = tokens[FlightHeader.DEST];
    int year = Integer.parseInt(tokens[FlightHeader.YEAR]);
    int month = Integer.parseInt(tokens[FlightHeader.MONTH]);
    String cancelled = tokens[FlightHeader.CANCELLED];
    String diverted = tokens[FlightHeader.DIVERTED];
    String flightDate = tokens[FlightHeader.FLIGHT_DATE];

    //Only consider non-cancelled and non-diverted flights
    if (!(cancelled.equals(Configuration.EXPECTED_CANCELLED_STATUS)
        && diverted.equals(Configuration.EXPECTED_DIVERTED_STATUS))) {
      return;
    }

    //Only consider flights within the defined time range
    if ((year > Configuration.INTERVAL_START_YEAR || (year == Configuration.INTERVAL_START_YEAR
        && month >= Configuration.INTERVAL_START_MONTH))
        &&
        (year < Configuration.INTERVAL_END_YEAR || (year == Configuration.INTERVAL_END_YEAR
            && month <= Configuration.INTERVAL_END_MONTH))) {
      String transitAirport;
      if (origin.equals(Configuration.ORIGIN_AIRPORT) && !dest.equals(Configuration.DEST_AIRPORT)){
        transitAirport = dest;
      }
      else if (!origin.equals(Configuration.ORIGIN_AIRPORT) && dest.equals(Configuration.DEST_AIRPORT)){
        transitAirport = origin;
      }
      else return;


      //Filtering necessary data only
      String depTime = tokens[FlightHeader.DEP_TIME];
      String arrTime = tokens[FlightHeader.ARR_TIME];
      int arrDelayMinutes = (int) Math.round(Double.parseDouble(tokens[FlightHeader.ARR_DELAY_MINUTES]));

      FlightValue fValue = new FlightValue(origin, dest, depTime, arrTime, arrDelayMinutes);
      context.write(new Text(String.format("%s,%s", flightDate, transitAirport)), fValue);
    }
  }
}
