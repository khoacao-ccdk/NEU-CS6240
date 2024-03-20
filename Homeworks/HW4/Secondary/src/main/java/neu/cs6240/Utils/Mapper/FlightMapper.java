package neu.cs6240.Utils.Mapper;

import com.opencsv.CSVParser;
import java.io.IOException;
import neu.cs6240.Utils.Common;
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
    if(!Common.isValidRecord(tokens))
      return;

    String month = tokens[FlightHeader.MONTH];
    String airline = tokens[FlightHeader.AIRLINE];
    int arrDelayMinutes = (int) Math.round(Double.parseDouble(tokens[FlightHeader.ARR_DELAY_MINUTES]));

    FlightKey fKey = new FlightKey(airline, month);
    context.write(fKey, new IntWritable(arrDelayMinutes));
  }

}
