package neu.cs6240.Utils.Mapper;

import neu.cs6240.Utils.Common;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HComputeMapper extends Mapper<ImmutableBytesWritable, Writable, Text, Text> {
    private Table fTable;
    private Connection conn;

    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Writable, Text, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        try{
            org.apache.hadoop.conf.Configuration hBaseconf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(hBaseconf);
            fTable = conn.getTable(TableName.valueOf(Common.HBASE_TABLE));
        }
        catch (Exception e){
            e.printStackTrace();
            System.exit(2);
        }
    }

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

    }

    @Override
    protected void cleanup(Mapper<ImmutableBytesWritable, Writable, Text, Text>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        fTable.close();
        conn.close();
    }
}
