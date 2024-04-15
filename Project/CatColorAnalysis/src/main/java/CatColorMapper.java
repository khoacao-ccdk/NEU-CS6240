import Utils.CatColorKey;
import Utils.ColorExtractor;
import Utils.ImageWrapper;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.imageio.ImageIO;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.google.gson.Gson;
import com.opencsv.CSVParser;
import Utils.CatHeader;

public class CatColorMapper extends Mapper<Object, Text, CatColorKey, IntWritable> {

  Map<CatColorKey, Integer> map = new HashMap<>(); //In-mapper combiner

  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    CSVParser parser = new CSVParser();
    String[] tokens = parser.parseLine(value.toString());

    // Parse the JSON string using Gson
    String breed = tokens[CatHeader.BREED];

    // Access the first image and extract the full URL
    String fullImageUrl = tokens[CatHeader.PHOTOS].replaceAll(" ", "%20");

    URL imageURL = new URL(fullImageUrl);
    BufferedImage image = null;
    try {
      image = ImageIO.read(imageURL);
    } catch (Exception e) {
      //In the event that the image could not be retrieved, skip this map
      return;
    }

    Set<String> colors = ColorExtractor.extractColor(image);

    for (String color : colors) {
      CatColorKey cKey = new CatColorKey(breed, color);
      map.compute(cKey, (k, v) -> (v == null) ? 1 : v + 1);
    }
    //Along with the color key, a dummy key is also created for the purpose of order inversion.
    //However, the dummy key is only created once to represent one cat that might have more than 1 color on their fur
    CatColorKey dummyKey = CatColorKey.createDummy(breed);
    map.compute(dummyKey, (k, v) -> (v == null) ? 1 : v + 1);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    //Emits the key-value pair after the map finishes
    for (Entry<CatColorKey, Integer> record : map.entrySet()) {
      context.write(record.getKey(), new IntWritable(record.getValue()));
    }
  }
}
