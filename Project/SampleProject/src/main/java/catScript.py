import sys
import io
from PIL import Image
from transformers import DetrImageProcessor, DetrForObjectDetection, pipeline

def main():
    # Path to input image
    image_path = sys.argv[1]

    # Open the image
    image = Image.open(image_path)

    # Initialize the object detection model
    processor = DetrImageProcessor.from_pretrained("facebook/detr-resnet-50", revision="no_timm")
    object_detection_model = DetrForObjectDetection.from_pretrained("facebook/detr-resnet-50", revision="no_timm")

    # Process the image
    inputs = processor(images=image, return_tensors="pt")
    outputs = object_detection_model(**inputs)

    # Convert outputs to COCO API
    target_sizes = torch.tensor([image.size[::-1]])
    results = processor.post_process_object_detection(outputs, target_sizes=target_sizes, threshold=0.9)[0]

    # Crop and classify each cat
    box_list = results["boxes"].tolist()
    labels = [object_detection_model.config.id2label.get(key) for key in results["labels"].tolist()]
    cat_breeds = []

    for i, label in enumerate(labels):
        if label == "cat":
            cat_image = image.crop(box_list[i])
            cat_bytes = io.BytesIO()
            cat_image.save(cat_bytes, format='JPEG')
            cat_bytes.seek(0)
            classifier = pipeline("image-classification", model="checkpoint-16815")
            output = classifier(cat_bytes)[0]
            cat_breeds.append(output['label'])

    # Output cat breeds with counts
    breed_counts = {breed: cat_breeds.count(breed) for breed in cat_breeds}
    for breed, count in breed_counts.items():
        print(f"{breed} {count}")

if __name__ == "__main__":
    main()
