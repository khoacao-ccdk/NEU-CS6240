import Utils.ImageWrapper;
import de.androidpit.colorthief.ColorThief;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;
import javax.imageio.ImageIO;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.google.gson.Gson;
import com.opencsv.CSVParser;
import Utils.CatHeader;
public class CatColorMapper extends Mapper<Object, Text, Text, Text> {
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    CSVParser parser = new CSVParser();
    String[] tokens = parser.parseLine(value.toString());

    // Parse the JSON string using Gson
    Gson gson = new Gson();
    String breed = tokens[CatHeader.BREED];
    ImageWrapper[] images = gson.fromJson(tokens[CatHeader.PHOTOS], ImageWrapper[].class);

    // Access the first image and extract the full URL
    String fullImageUrl = images[0].getFull();

    URL imageURL = new URL(fullImageUrl);
    BufferedImage image = ImageIO.read(imageURL);
    int[] color = ColorThief.getColor(image);

    // Print the extracted primary color
    System.out.printf("[%d, %d, %d]\n", color[0], color[1], color[2]);
  }
}
