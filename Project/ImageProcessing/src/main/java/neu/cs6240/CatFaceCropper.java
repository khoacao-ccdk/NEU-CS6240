package neu.cs6240;

import java.io.File;
import java.io.IOException;
import java.util.List;

import nu.pattern.OpenCV;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.MatOfRect;
import org.opencv.core.Point;
import org.opencv.core.Rect;
import org.opencv.core.Scalar;
import org.opencv.core.Size;
import org.opencv.highgui.HighGui;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.objdetect.CascadeClassifier;

public class CatFaceCropper {

    public static void detectAndCrop(String imagePath, String catCascadePath) throws IOException {
        // Load the image
        Mat image = Imgcodecs.imread(imagePath);

        // Check if image loaded successfully
        if (image.empty()) {
            System.err.println("Error: Image could not be loaded - " + imagePath);
            return;
        }

        // Load the cat face cascade classifier
        CascadeClassifier catCascade = new CascadeClassifier();
        if (!catCascade.load(catCascadePath)) {
            System.err.println("Error: Could not load cat cascade classifier - " + catCascadePath);
            return;
        }

        // Convert image to grayscale for detection
        Mat imageGray = new Mat();
        Imgproc.cvtColor(image, imageGray, Imgproc.COLOR_BGR2GRAY);
        Imgproc.equalizeHist(imageGray, imageGray);

        // Detect cat faces
        MatOfRect catFaces = new MatOfRect();
        catCascade.detectMultiScale(imageGray, catFaces, 1.1, 3);
        List<Rect> listOfFaces = catFaces.toList();
        System.out.printf("Detected %d cat face(s) \n", listOfFaces.size());
        Rect enlargedRect = null;

        int faceNo = 1;

        /// Process each detected cat face
        // Process each detected cat face
        for (Rect face : listOfFaces) {
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

            //Save the cropped face image
            String croppedImagePath =  (faceNo++) + "_cropped_face.jpg";
            Imgcodecs.imwrite(croppedImagePath, croppedFace);
        }
    }

    public static void main(String[] args) throws IOException {
        // Load the native OpenCV library
        OpenCV.loadLocally();

        // Replace with your actual list of cat image paths
        List<String> catImageList = List.of(
                CatFaceCropper.class.getResource("/cat_data_small/0160_005.JPG").getFile()
        );

        // Path to your cat face cascade classifier (frontcalcatface.xml)
        String catCascadePath = CatFaceCropper.class.getResource("/haarcascades/haarcascade_frontalcatface_extended.xml").getFile();

        // Process each cat image in the list
        for (String imagePath : catImageList) {
            detectAndCrop(imagePath, catCascadePath);
        }

        // Release resources
        System.exit(0);
    }
}
