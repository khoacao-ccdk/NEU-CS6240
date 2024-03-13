package Utils.Mappers;

import static Utils.Configuration.VALID_START_WORDS;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class - Filtering "real" words and emit them
 */
public class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable> {
  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();

  public void map(Object key, Text value, Context context
  ) throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
    while (itr.hasMoreTokens()) {
      word.set(itr.nextToken());
      char firstChar = word.toString().charAt(0);
      //Only emits the word if it starts with a valid character
      if(VALID_START_WORDS.contains(firstChar)) {
        context.write(word, one);
      }
    }
  }
}
