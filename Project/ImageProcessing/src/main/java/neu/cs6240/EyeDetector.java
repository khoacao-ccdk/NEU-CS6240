package neu.cs6240;

import java.io.IOException;
import java.util.List;

import nu.pattern.OpenCV;
import org.opencv.core.Mat;
import org.opencv.core.MatOfRect;
import org.opencv.core.Point;
import org.opencv.core.Rect;
import org.opencv.core.Scalar;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.objdetect.CascadeClassifier;

public class EyeDetector {

    public static void detectEye(String imagePath, String faceCascadePath, String eyeCascadePath) throws IOException {
        // Load the image
        Mat image = Imgcodecs.imread(imagePath);

        // Check if image loaded successfully
        if (image.empty()) {
            System.err.println("Error: Image could not be loaded - " + imagePath);
            return;
        }

        // Load the cascade classifiers
        CascadeClassifier faceCascade = new CascadeClassifier();
        CascadeClassifier eyeCascade = new CascadeClassifier();
        if (!faceCascade.load(faceCascadePath) || !eyeCascade.load(eyeCascadePath)) {
            System.err.println("Error: Could not load cascade classifiers");
            return;
        }

        // Convert image to grayscale for detection
        Mat imageGray = new Mat();
        Imgproc.cvtColor(image, imageGray, Imgproc.COLOR_BGR2GRAY);
        Imgproc.equalizeHist(imageGray, imageGray);

        // Detect cat faces
        MatOfRect catFaces = new MatOfRect();
        faceCascade.detectMultiScale(imageGray, catFaces);
        List<Rect> listOfFaces = catFaces.toList();

        System.out.printf("Detected %d cat(s) \n", listOfFaces.size());

        int faceNo = 1;

        // Process each detected face
        for (Rect face : listOfFaces) {
            // Detect eyes within the face region
            Mat faceROI = imageGray.submat(face);
            MatOfRect eyes = new MatOfRect();
            eyeCascade.detectMultiScale(faceROI, eyes);
            List<Rect> listOfEyes = eyes.toList();

            System.out.printf("Detected %d eye(s) \n", listOfEyes.size());

            // Process each detected eye
            for (Rect eye : listOfEyes) {
                // Adjust coordinates for eye rectangle relative to the original image
                int eyeX = face.x + eye.x;
                int eyeY = face.y + eye.y;
                int eyeWidth = eye.width;
                int eyeHeight = eye.height;

                // Draw the modified rectangle
                Imgproc.rectangle(image, new Point(eyeX, eyeY),
                        new Point(eyeX + eyeWidth, eyeY + eyeHeight),
                        new Scalar(0, 255, 0), 2); // Green rectangle for eyes

                //Save the cropped face image
                String croppedImagePath =  (faceNo++) + "_cropped_eye.jpg";
                Imgcodecs.imwrite(croppedImagePath, image);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        // Load the native OpenCV library
        OpenCV.loadLocally();

        // Replace with your actual list of cat image paths
        List<String> catImageList = List.of(
                CatFaceCropper.class.getResource("/cat_data_small/thom.jpg").getFile()
        );

        // Path to your cat face cascade classifier (frontcalcatface.xml)
        String catFaceCascadePath = CatFaceCropper.class.getResource("/haarcascades/haarcascade_frontalface_alt.xml").getFile();
        String catEyeCascadePath = CatFaceCropper.class.getResource("/haarcascades/haarcascade_eye_tree_eyeglasses.xml").getFile();

        // Process each cat image in the list
        for (String imagePath : catImageList) {
            detectEye(imagePath, catFaceCascadePath, catEyeCascadePath);
        }

        // Release resources
        System.exit(0);
    }
}
