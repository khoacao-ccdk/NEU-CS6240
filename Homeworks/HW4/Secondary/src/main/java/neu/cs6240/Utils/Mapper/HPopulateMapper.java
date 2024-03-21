package neu.cs6240.Utils.Mapper;

import com.opencsv.CSVParser;
import neu.cs6240.Utils.Common;
import neu.cs6240.Utils.FlightHeader;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class HPopulateMapper extends Mapper<Object, Text, ImmutableBytesWritable, Writable> {
    private static final long FLUSH_PERIOD = Duration.of(30, ChronoUnit.SECONDS).toMillis();
    private Table fTable;
    private Connection conn;
    private BufferedMutator mutator;
    @Override
    protected void setup(Mapper<Object, Text, ImmutableBytesWritable, Writable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        try{
            org.apache.hadoop.conf.Configuration hBaseconf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(hBaseconf);
            fTable = conn.getTable(TableName.valueOf(Common.HBASE_TABLE));

            mutator = conn.getBufferedMutator(TableName.valueOf(Common.HBASE_TABLE));
            mutator.setWriteBufferPeriodicFlush(FLUSH_PERIOD); //Set a flush duration for buffered write to the table
        }
        catch (Exception e){
            e.printStackTrace();
            System.exit(2);
        }
    }

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        CSVParser parser = new CSVParser();
        String[] tokens = parser.parseLine(value.toString());

        //Pulling necessary fields to use as key
        String airline = tokens[FlightHeader.AIRLINE];
        String month = tokens[FlightHeader.MONTH];
        String flightDate = tokens[FlightHeader.FLIGHT_DATE];
        String flightNumber = tokens[FlightHeader.FLIGHT_NUM];
        String origin = tokens[FlightHeader.ORIGIN];

        /**
         * Key consists of the airline name, month, date of flight, and flight number
         * - Airline and Month is used to sort and group the keys
         * - flightDate, flightNumber, and origin is used to uniquely identify the flights
         */
        String fKey = new StringBuilder()
                .append(airline).append("-")
                .append(month).append("-")
                .append(flightDate).append("-")
                .append(flightNumber).append("-")
                .append(origin)
                .toString();
        Put putRequest = new Put(Bytes.toBytes(fKey));

        //Put the whole record into the "FlightData" column. This data will be parsed later in teh HCompute step
        putRequest.addColumn(
            Bytes.toBytes(Common.HBASE_COL_FAMILY),
            Bytes.toBytes(Common.HBASE_VAL_COL_NAME),
            Bytes.toBytes(value.toString())
        );

        mutator.mutate(putRequest);
    }

    @Override
    protected void cleanup(Mapper<Object, Text, ImmutableBytesWritable, Writable>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        mutator.flush(); //Flush the pending put requests
        mutator.close();
        fTable.close();
        conn.close();
    }
}
