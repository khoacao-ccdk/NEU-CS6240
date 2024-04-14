import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CatRecognitionMRJob {

  public static class CatRecognitionMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Get the path to the input image file
      String imagePath = value.toString();

      // Execute the Python script to perform cat recognition and breed identification
      ProcessBuilder processBuilder = new ProcessBuilder("python", "catScript.py", imagePath);
      Process process = processBuilder.start();

      // Read the output of the Python script
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        // Parse the output and emit cat breeds
        String[] parts = line.split("\\s+");
        if (parts.length == 2) {
          String breed = parts[0];
          int count = Integer.parseInt(parts[1]);
          context.write(new Text(breed), new IntWritable(count));
        }
      }

      // Wait for the process to finish
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        // Handle error
      }
    }
  }

  public static class CatRecognitionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      // Aggregate counts for each cat breed
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }
}
