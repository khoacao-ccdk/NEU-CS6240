package neu.cs6240;

import neu.cs6240.Utils.Common;
import neu.cs6240.Utils.Mapper.HPopulateMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

import static neu.cs6240.Utils.Common.HBASE_COL_FAMILY;
import static neu.cs6240.Utils.Common.HBASE_TABLE;

public class HPopulate {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = HBaseConfiguration.create(new Configuration());
        String[] arguments = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(arguments.length != 2){
            System.err.println("To use this, provide the following: arguments <in>, <out>");
            System.exit(2);
        }
        //create table
        createTable();

        //Config MapReduce job
        Job job = Job.getInstance(conf, "Average Delay - HPopulate");
        job.setJarByClass(HCompute.class);
        job.setMapperClass(HPopulateMapper.class);
        job.setNumReduceTasks(0);

        //Set output to write to HBase
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, HBASE_TABLE);

        // set partitioner to 0 explicitly since there's no reduce task needed
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(arguments[0]));
        FileOutputFormat.setOutputPath(job, new Path(arguments[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * This function creates a new table in HBase
     */
    private static void createTable() {
        Configuration hBaseconf = HBaseConfiguration.create();
        try(Connection conn = ConnectionFactory.createConnection(hBaseconf)){
            Admin admin = conn.getAdmin();

            //Configurations for the table
            TableName tName = TableName.valueOf(HBASE_TABLE);
            TableDescriptorBuilder descriptor = TableDescriptorBuilder
                    .newBuilder(tName)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HBASE_COL_FAMILY));

            //Delete if already exists
            if(admin.tableExists(tName)){
                admin.disableTable(tName);
                admin.deleteTable(tName);
            }

            admin.createTable(descriptor.build());
            admin.close();
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("Could not create connection!");
            System.exit(1);
        }
    }
}
