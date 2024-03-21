package neu.cs6240.Utils.Mapper;

import com.opencsv.CSVParser;
import neu.cs6240.Utils.Common;
import neu.cs6240.Utils.FlightHeader;
import neu.cs6240.Utils.FlightKey;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * This Mapper class performs the logic almost identical to that of FlightMapper. The difference is in how
 * it reads the value 
 */
public class HComputeMapper extends TableMapper<FlightKey, IntWritable> {

    public void map(ImmutableBytesWritable row, Result value, Context context)
            throws IOException, InterruptedException {
        CSVParser parser = new CSVParser();

        //Parse the column to String
        String rowValue = new String((value.getValue(
                Bytes.toBytes(Common.HBASE_COL_FAMILY),
                Bytes.toBytes(Common.HBASE_VAL_COL_NAME)
        )), StandardCharsets.UTF_8);

        String[] tokens = parser.parseLine(rowValue);
        if (!Common.isValidRecord(tokens))
            return;

        String month = tokens[FlightHeader.MONTH];
        String airline = tokens[FlightHeader.AIRLINE];
        int arrDelayMinutes = (int) Math.round(Double.parseDouble(tokens[FlightHeader.ARR_DELAY_MINUTES]));

        FlightKey fKey = new FlightKey(airline, month);
        context.write(fKey, new IntWritable(arrDelayMinutes));
    }
}
