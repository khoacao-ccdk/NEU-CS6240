package neu.cs6240.Utils.Mapper;

import com.opencsv.CSVParser;
import java.io.IOException;
import neu.cs6240.Utils.Configuration;
import neu.cs6240.Utils.FlightHeader;
import neu.cs6240.Utils.FlightKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper extends Mapper<Object, Text, FlightKey, IntWritable> {
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    CSVParser parser = new CSVParser();
    String[] tokens = parser.parseLine(value.toString());
    if(!isValidRecord(tokens))
      return;

    String month = tokens[FlightHeader.MONTH];
    String airline = tokens[FlightHeader.AIRLINE];
    int arrDelayMinutes = (int) Math.round(Double.parseDouble(tokens[FlightHeader.ARR_DELAY_MINUTES]));

    FlightKey fKey = new FlightKey(airline, month);
    context.write(fKey, new IntWritable(arrDelayMinutes));
  }

  /**
   * Checks if the provided record is valid
   * @param tokens a String array of the parsed line using CSVParser
   * @return whether the record is valid
   */
  private boolean isValidRecord(String[] tokens) {
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
    if(year != Configuration.EXPECTED_YEAR || !cancelled.equals(Configuration.EXPECTED_CANCELLED_STATUS))
      return false;

    return true;
  }
}
