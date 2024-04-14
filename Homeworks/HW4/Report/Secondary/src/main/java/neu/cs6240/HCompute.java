package neu.cs6240;

import neu.cs6240.Utils.Common;
import neu.cs6240.Utils.Extras.FlightGroupComparator;
import neu.cs6240.Utils.Extras.FlightKeyComparator;
import neu.cs6240.Utils.Extras.FlightPartitioner;
import neu.cs6240.Utils.FlightKey;
import neu.cs6240.Utils.Mapper.HComputeMapper;
import neu.cs6240.Utils.Reducer.FlightReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class HCompute {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = HBaseConfiguration.create(new Configuration());
        conf.set("hbase.zookeeper.quorum", Common.HBASE_ZOOKEEPER_QUORUM_ADDRESS);
        conf.set("hbase.zookeeper.property.clientPort", Common.HBASE_PORT);

        String[] arguments = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(arguments.length != 2){
            System.err.println("To use this, provide the following: arguments <in>, <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Average Delay - HCompute");
        job.setJarByClass(HCompute.class);     // class that contains mapper

        //Set up a filter condition
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter yearFilter = new SingleColumnValueFilter(
                Common.HBASE_COL_FAMILY.getBytes(),
                Common.HBASE_YEAR_COL_NAME.getBytes(),
                CompareOperator.EQUAL,
                Bytes.toBytes(Common.EXPECTED_YEAR));
        filterList.addFilter(yearFilter);

        SingleColumnValueFilter cancelledFilter = new SingleColumnValueFilter(
                Common.HBASE_COL_FAMILY.getBytes(),
                Common.HBASE_CANCELLED_COL_NAME.getBytes(),
                CompareOperator.EQUAL,
                Common.EXPECTED_CANCELLED_STATUS.getBytes());
        filterList.addFilter(cancelledFilter);

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        scan.setFilter(filterList);  // Set filter conditions for the scan job

        TableMapReduceUtil.initTableMapperJob(
                Common.HBASE_TABLE,        // input HBase table name
                scan,                      // Scan instance to control CF and attribute selection
                HComputeMapper.class,      // mapper
                FlightKey.class,           // mapper output key
                Text.class,                // mapper output value
                job);
        job.setMapOutputKeyClass(FlightKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        //The rest is similar to that of the Secondary Sort MR config
        job.setReducerClass(FlightReducer.class);
        job.setPartitionerClass(FlightPartitioner.class);
        job.setSortComparatorClass(FlightKeyComparator.class);
        job.setGroupingComparatorClass(FlightGroupComparator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(arguments[0]));
        FileOutputFormat.setOutputPath(job, new Path(arguments[1]));
        System.out.println(arguments[1]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
