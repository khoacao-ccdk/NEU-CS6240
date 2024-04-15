import Utils.CatColorKey;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CatColorReducer extends Reducer<CatColorKey, IntWritable, Text, DoubleWritable> {
  long marginal;
  String currBreed;

  public void reduce(CatColorKey key, Iterable<IntWritable> values,Context context)
      throws IOException, InterruptedException {
    //If a new breed is seen, reset the counter
    if(!key.getBreed().equals(currBreed)) {
      marginal = 0;
      currBreed = key.getBreed();
    }

    String currColor = null;
    double colorCount = 0;
    for(IntWritable v : values) {
      String color = key.getColor();
      if(!color.equals(currColor)) {
        DoubleWritable result = new DoubleWritable(1.0d * colorCount / marginal);
        if(currColor != null && !currColor.equals(CatColorKey.DUMMY_COLOR)) {
          context.write(
                  new Text(String.format("(%s, %s)", currBreed, currColor)),
                  result
          );
        }

        currColor = color;
        colorCount = 0;
      }
      if(color.equals(CatColorKey.DUMMY_COLOR)) {
        marginal += v.get();
      } else {
        colorCount += v.get();
      }
    }
  }
}
