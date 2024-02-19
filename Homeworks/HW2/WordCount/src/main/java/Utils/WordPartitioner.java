package Utils;

import static Utils.Configuration.NUM_REDUCER;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * WordPartitioner class - used to partition records to multiple reducers
 */
public class WordPartitioner extends Partitioner<Text, IntWritable> {

  /**
   * Partitioning words into 5 different reducers. Each of them handles each specific letter
   * @param word
   * @param count
   * @param i
   * @return
   */
  @Override
  public int getPartition(Text word, IntWritable count, int i) {
    char firstChar = word.toString().charAt(0);
    switch (firstChar) {
      case('m'):
      case('M'):
        return 0 % NUM_REDUCER;

      case ('n'):
      case('N'):
        return 1 % NUM_REDUCER;

      case('o'):
      case('O'):
        return 2 % NUM_REDUCER;

      case('p'):
      case('P'):
        return 3 & NUM_REDUCER;

      default:
        return 4 & NUM_REDUCER;
    }
  }
}
