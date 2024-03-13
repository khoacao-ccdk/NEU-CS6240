package neu.cs6240.Utils.Mappers;

import neu.cs6240.Utils.Configuration;
import neu.cs6240.Utils.FlightHeader;
import neu.cs6240.Utils.FlightKey;

import neu.cs6240.Utils.FlightValue;
import com.opencsv.CSVParser;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class FlightMapper extends Mapper<Object, Text, FlightKey, FlightValue> {
  private Logger logger = Logger.getLogger(FlightMapper.class);

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
      System.out.println("Skipping due to cancelled/diverted");
      logger.error("Skipping due to cancelled/diverted");
      return;
    }

    //Only consider flights within the defined time range
    if ((year > Configuration.INTERVAL_START_YEAR || (year == Configuration.INTERVAL_START_YEAR
        && month >= Configuration.INTERVAL_START_MONTH))
        &&
        (year < Configuration.INTERVAL_END_YEAR || (year == Configuration.INTERVAL_END_YEAR
            && month <= Configuration.INTERVAL_END_MONTH))) {
      FlightKey fKey;


      if (origin.equals(Configuration.ORIGIN_AIRPORT) && !dest.equals(Configuration.DEST_AIRPORT)) {
        fKey = new FlightKey(flightDate, FlightKey.LEG_ONE);
      } else if (!origin.equals(Configuration.ORIGIN_AIRPORT) && dest.equals(Configuration.DEST_AIRPORT)) {
        fKey = new FlightKey(flightDate, FlightKey.LEG_TWO);
      } else {
        return;
      }

      //Filtering necessary data only
      String depTime = tokens[FlightHeader.DEP_TIME];
      String arrTime = tokens[FlightHeader.ARR_TIME];
      int arrDelayMinutes = Integer.parseInt(tokens[FlightHeader.ARR_DELAY_MINUTES]);

      FlightValue fValue = new FlightValue(origin, dest, depTime, arrTime, arrDelayMinutes);
      //Emits the key-value pair
      context.write(fKey, fValue);
    }
  }
}
