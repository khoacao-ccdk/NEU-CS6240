package Utils.Mappers;

import static Utils.Configuration.VALID_START_WORDS;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * TokenizerMapperPerTaskTally class - Filtering "real" words and emit them
 *
 * A HashMap is used to aggregate the count of every word iterated throughout the instance's lifecycle
 */
public class TokenizerMapperPerTaskTally
    extends Mapper<Object, Text, Text, IntWritable> {
  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private Map<Text, IntWritable> countMap = new HashMap<>();

  public void map(Object key, Text value, Context context
  ) throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
    while (itr.hasMoreTokens()) {
      word.set(itr.nextToken());
      char firstChar = word.toString().charAt(0);
      //Only emits the word if it starts with a valid character
      if(VALID_START_WORDS.contains(firstChar)) {
        IntWritable currCount = countMap.getOrDefault(word, new IntWritable(0));

        //Increment count
        currCount.set(currCount.get() + 1);
        countMap.put(new Text(word.toString()), currCount);
      }
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    // Emits the final counter values after all records have been processed in the input split
    for (Entry<Text, IntWritable> record : countMap.entrySet()) {
      context.write(record.getKey(), record.getValue());
    }
  }
}
