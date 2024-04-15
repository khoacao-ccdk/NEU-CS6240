package Utils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;


public class KeyPartitioner extends Partitioner <CatColorKey, IntWritable>{
    @Override
    public int getPartition(CatColorKey key, IntWritable intWritable, int numPartitions) {
        return Math.abs(key.getBreed().hashCode()) % numPartitions;
    }
}

