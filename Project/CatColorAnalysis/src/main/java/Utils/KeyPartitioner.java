package Utils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;


public class KeyPartitioner extends Partitioner <CatColorKey, IntWritable>{
    @Override
    public int getPartition(CatColorKey key, IntWritable intWritable, int numPartitions) {
        int partition = Math.abs(key.getBreed().hashCode()) % numPartitions;
        System.out.printf("Key %s goes to partition %d", key, partition);
        return partition;
    }
}

