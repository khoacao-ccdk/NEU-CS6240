package neu.cs6240;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;

import de.androidpit.colorthief.ColorThief;

import javax.imageio.ImageIO;

public class ColorExtractor {

    public static void main(String[] args) throws IOException {
        URL imageURL = ColorExtractor.class.getResource("/cat_data_small/0021_035.JPG");
        BufferedImage image = ImageIO.read(imageURL);
        int[] color = ColorThief.getColor(image);
        System.out.printf("[%d, %d, %d]\n", color[0], color[1], color[2]);
    }
}
