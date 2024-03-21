package neu.cs6240;

import neu.cs6240.Utils.Common;
import neu.cs6240.Utils.Extras.FlightGroupComparator;
import neu.cs6240.Utils.Extras.FlightKeyComparator;
import neu.cs6240.Utils.Extras.FlightPartitioner;
import neu.cs6240.Utils.FlightKey;
import neu.cs6240.Utils.Mapper.HComputeMapper;
import neu.cs6240.Utils.Reducer.FlightReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

public class HCompute {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "Average Delay - HCompute");
        job.setJarByClass(HCompute.class);     // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        TableMapReduceUtil.initTableMapperJob(
                Common.HBASE_TABLE,        // input HBase table name
                scan,                      // Scan instance to control CF and attribute selection
                HComputeMapper.class,      // mapper
                FlightKey.class,           // mapper output key
                Text.class,                // mapper output value
                job);
        job.setOutputFormatClass(NullOutputFormat.class);

        //The rest is similar to that of the Secondary Sort MR config
        job.setReducerClass(FlightReducer.class);
        job.setPartitionerClass(FlightPartitioner.class);
        job.setSortComparatorClass(FlightKeyComparator.class);
        job.setGroupingComparatorClass(FlightGroupComparator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
