package Utils;

import de.androidpit.colorthief.ColorThief;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ColorExtractor {
  private static final int NUM_COLOR = 2; //Number of top occured color to extract
  //A list of 10 most basic colors that might appear on a cat's fur
  public static final Map<String, int[]> BASIC_COLOR_MAP;

  static {
    BASIC_COLOR_MAP = new HashMap<>();
    BASIC_COLOR_MAP.put("Black", new int[]{0, 0, 0});
    BASIC_COLOR_MAP.put("White", new int[]{255, 255, 255});
    BASIC_COLOR_MAP.put("Gray", new int[]{128, 128, 128});
    BASIC_COLOR_MAP.put("Blue Gray", new int[]{176, 196, 222});
    BASIC_COLOR_MAP.put("Cream", new int[]{245, 222, 179});
    BASIC_COLOR_MAP.put("Reddish Orange", new int[]{230, 126, 34});
    BASIC_COLOR_MAP.put("Light Orange", new int[]{255, 165, 0});
    BASIC_COLOR_MAP.put("Brown", new int[]{139, 69, 19});
    BASIC_COLOR_MAP.put("Red", new int[]{255, 0, 0});
    BASIC_COLOR_MAP.put("Silver", new int[]{192, 192, 205});
  }

  /**
   * This function clips the triplet (r,g,b) to the closest color value
   * @param triplet
   * @return
   */
  private static String getClosestColorValue(int[] triplet) {

    // Calculate the squared distance to each color
    Map<String, Double> distances = new HashMap<>();
    for (String colorName : BASIC_COLOR_MAP.keySet()) {
      int[] color = BASIC_COLOR_MAP.get(colorName);
      double distance = Math.pow(triplet[0] - color[0], 2)
          + Math.pow(triplet[1] - color[1], 2)
          + Math.pow(triplet[2] - color[2], 2);
      distances.put(colorName, distance);
    }

    // Find the color with the minimum distance
    String closestColor = null;
    double minDistance = Double.MAX_VALUE;
    for (String colorName : distances.keySet()) {
      double distance = distances.get(colorName);
      if (distance < minDistance) {
        minDistance = distance;
        closestColor = colorName;
      }
    }

    return closestColor;
  }

  /**
   * This function performs the step of detecting the cat's face, cropping it, and determine the
   * major color on the cat's face. In the event the crop step could not be completed, the original
   * image will be used. This might result in a lower accuracy
   * @param image a BufferedImage object represents the cat Image
   * @return A String of the triplet value (r,g,b) represents the cat color
   * @throws IOException
   */
  public static Set<String> extractColor(BufferedImage image) throws IOException {
    int[][] topColors = ColorThief.getPalette(image, NUM_COLOR);
    Set<String> closestBasicColors = new HashSet<>();
    for(int[]triplet : topColors) {
      closestBasicColors.add(getClosestColorValue(triplet));
    }
    return closestBasicColors;
  }
}
