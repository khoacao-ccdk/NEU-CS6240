package neu.cs6240.Utils;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Common {
  public static final int EXPECTED_YEAR = 2008;
  public static final String EXPECTED_CANCELLED_STATUS = "0.00";
  public static final String HBASE_ZOOKEEPER_QUORUM_ADDRESS = "ip-10-0-16-199.us-west-2.compute.internal";
  public static final String HBASE_PORT = "2181";
  public static final String HBASE_TABLE = "Flight_Record";
  public static final String HBASE_COL_FAMILY = "Flights";
  public static final String HBASE_VAL_COL_NAME = "RowText";
  public static final String HBASE_YEAR_COL_NAME = "year";
  public static final String HBASE_CANCELLED_COL_NAME = "cancelled";
  public static final long HBASE_FLUSH_PERIOD = Duration.of(1, ChronoUnit.MINUTES).toMillis();
  /**
   * A common method that can be called and reused by Mappers/Reducers to validate the record
   * @param tokens a list of String that represents the parsed record row
   * @return whether the record is valid to perform computation
   */
  public static boolean isValidRecord(String[] tokens) {
    //If any of the required field is empty, return false
    if(tokens[FlightHeader.YEAR].isEmpty() ||
            tokens[FlightHeader.MONTH].isEmpty() ||
            tokens[FlightHeader.FLIGHT_DATE].isEmpty() ||
            tokens[FlightHeader.AIRLINE].isEmpty() ||
            tokens[FlightHeader.ARR_DELAY_MINUTES].isEmpty() ||
            tokens[FlightHeader.CANCELLED].isEmpty()
    )
      return false;

    int year = Integer.parseInt(tokens[FlightHeader.YEAR]);
    String cancelled = tokens[FlightHeader.CANCELLED];

    //If the year is not the expected year or the flight is cancelled, return false
    if(year != Common.EXPECTED_YEAR || !cancelled.equals(Common.EXPECTED_CANCELLED_STATUS))
      return false;

    return true;
  }
}
