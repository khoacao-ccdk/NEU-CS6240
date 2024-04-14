import static Utils.ColorExtractor.extractColor;

import Utils.ImageWrapper;
import com.google.gson.Gson;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import javax.imageio.ImageIO;
import nu.pattern.OpenCV;

public class CatColorAnalysis {
  public static void main(String[] args) throws IOException {
    OpenCV.loadLocally();
    Scanner sc = new Scanner(System.in);
    String jsonString = sc.nextLine();
    sc.close();

    // Parse the JSON string using Gson
    Gson gson = new Gson();
    ImageWrapper[] images = gson.fromJson(jsonString, ImageWrapper[].class);

    // Access the first image and extract the full URL
    String fullImageUrl = images[0].getFull();

    // Print the full image URL
    URL imageURL = new URL(fullImageUrl);
    System.out.println(fullImageUrl);
    BufferedImage image = ImageIO.read(imageURL);

    Set<String> extractedColor = extractColor(image);
    for(String color : extractedColor) {
      System.out.println(color);
    }
  }

}
