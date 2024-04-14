import Utils.CatColorKey;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CatColorReducer extends Reducer<CatColorKey, IntWritable, CatColorKey, DoubleWritable> {
  long marginal;
  public void reduce(CatColorKey key, Iterable<IntWritable> values,Context context)
      throws IOException, InterruptedException {
    String color = key.getColor();
    if(color.equals(CatColorKey.DUMMY_COLOR)) {
      marginal = 0;
      for(IntWritable v : values) {
        marginal += v.get();
      }
    } else {
      long colorCount = 0;
      for(IntWritable v : values) {
        colorCount += v.get();
      }
      DoubleWritable result = new DoubleWritable(1.0d * colorCount / marginal);
      context.write(key, result);
    }
  }
}
