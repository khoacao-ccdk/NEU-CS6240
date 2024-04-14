package Utils;

import de.androidpit.colorthief.ColorThief;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.imageio.ImageIO;
import nu.pattern.OpenCV;
import org.apache.hadoop.util.hash.Hash;
import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.core.MatOfRect;
import org.opencv.core.Point;
import org.opencv.core.Rect;
import org.opencv.core.Scalar;
import org.opencv.core.Size;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.objdetect.CascadeClassifier;

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

  private static BufferedImage convertTo3ByteBGRType(BufferedImage image) {
    BufferedImage convertedImage = new BufferedImage(image.getWidth(), image.getHeight(),
        BufferedImage.TYPE_3BYTE_BGR);
    convertedImage.getGraphics().drawImage(image, 0, 0, null);
    return convertedImage;
  }
  private static Mat BufferedImage2Mat(BufferedImage image) throws IOException {
    image = convertTo3ByteBGRType(image);
    byte[] data = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
    Mat mat = new Mat(image.getHeight(), image.getWidth(), CvType.CV_8UC3);
    mat.put(0, 0, data);
    return mat;
  }
  private static BufferedImage Mat2BufferedImage(Mat matrix)throws IOException {
    MatOfByte mob=new MatOfByte();
    Imgcodecs.imencode(".jpg", matrix, mob);
    return ImageIO.read(new ByteArrayInputStream(mob.toArray()));
  }

  /**
   * This function use the haarcascades method to detect and crop the cat's face
   * @param image the original cat image
   * @return null if the model could not detect the cat's face, or the cropped image if it could
   * @throws IOException
   */
  private static Mat detectAndCrop(Mat image) throws IOException {
//    String catCascadePath = ColorExtractor.class
//        .getResource("/haarcascades/haarcascade_frontalcatface_extended.xml")
//        .getPath();
    InputStream cascadeStream = ColorExtractor.class.getResourceAsStream("/haarcascades/haarcascade_frontalcatface_extended.xml");
    File tempCascadeFile = File.createTempFile("haarcascade", ".xml");
    FileOutputStream tempOutStream = new FileOutputStream(tempCascadeFile);
    byte[] buffer = new byte[1024];
    int bytesRead;
    while ((bytesRead = cascadeStream.read(buffer)) > 0) {
      tempOutStream.write(buffer, 0, bytesRead);
    }
    tempOutStream.close();
    cascadeStream.close();

    String cascadePath = tempCascadeFile.getAbsolutePath();
    CascadeClassifier catCascade = new CascadeClassifier(cascadePath);

    // Load the cat face cascade classifier
//    CascadeClassifier catCascade = new CascadeClassifier();
//    if (!catCascade.load(catCascadePath)) {
//      System.err.println("Error: Could not load cat cascade classifier - " + catCascadePath);
//      return null;
//    }

    // Convert image to grayscale for detection
    Mat imageGray = new Mat();
    Imgproc.cvtColor(image, imageGray, Imgproc.COLOR_BGR2GRAY);
    Imgproc.equalizeHist(imageGray, imageGray);

    // Detect cat faces
    MatOfRect catFaces = new MatOfRect();
    catCascade.detectMultiScale(
        imageGray,
        catFaces,
        1.1,
        3,
        0,
        new Size(),
        new Size());
    List<Rect> listOfFaces = catFaces.toList();
    if(listOfFaces.isEmpty()) return null;

    Rect face = listOfFaces.get(0); //Only get the first cat that was detected
    // Crop the image based on the detected face rectangle
    Mat croppedFace = new Mat(image, face);
    int rectX = Math.max(0, face.x - 10);
    int rectY = Math.max(0, face.y - 10);
    int rectWidth = face.width + 20;
    int rectHeight = face.height + 20;

    // Draw the modified rectangle
    Imgproc.rectangle(image, new Point(rectX, rectY),
        new Point(rectX + rectWidth, rectY + rectHeight),
        new Scalar(0, 255, 0));

    tempCascadeFile.delete(); //Delete temp file after done
    return croppedFace;
  }

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
    OpenCV.loadLocally();
//    System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
    Mat catFace = detectAndCrop(BufferedImage2Mat(image));
    BufferedImage imageToAnalyze = catFace != null
        ? Mat2BufferedImage(catFace)
        : image;
    int[][] topColors = ColorThief.getPalette(imageToAnalyze, NUM_COLOR);
    Set<String> closestBasicColors = new HashSet<>();
    for(int[]triplet : topColors) {
      closestBasicColors.add(getClosestColorValue(triplet));
    }
    return closestBasicColors;
  }
}
