import Utils.CatHeader;
import Utils.ColorExtractor;
import Utils.ImageWrapper;
import com.google.gson.Gson;
import com.opencsv.CSVParser;
import java.awt.image.BufferedImage;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Scanner;
import java.util.Set;
import javax.imageio.ImageIO;

public class Test {
  public static void main(String[] args) throws Exception {
    Scanner sc = new Scanner(System.in);
    String url = sc.nextLine();
    sc.close();


    // Parse the JSON string using Gson
    Gson gson = new Gson();
    ImageWrapper[] images = gson.fromJson(url, ImageWrapper[].class);

    // Access the first image and extract the full URL
    String fullImageUrl = images[0].getFull();

    URL imageURL = new URL(fullImageUrl);
    BufferedImage image = null;
    try {
      image = ImageIO.read(imageURL);
    } catch (Exception e) {
      //In the event that the image could not be retrieved, skip this map
      return;
    }

    Set<String> colors = ColorExtractor.extractColor(image);
    for(String color : colors) {
      System.out.println(color);
    }
  }
}