import boto3
import csv
import os

# Configure S3 access (replace with your credentials)
s3_client = boto3.client('s3',
                          aws_access_key_id='<access-key>',
                          aws_secret_access_key='<secret-key')
bucket_name = '<bucket-name'  # Replace with your S3 bucket name

# Define base data directory (replace with actual path)
data_dir = "<folder-path>"


def upload_image_get_url(image_path, object_key):
  s3_client.upload_file(image_path, bucket_name, object_key)
  image_url = f"https://{bucket_name}.s3.amazonaws.com/{object_key}"
  return image_url


# Open CSV file for writing image URLs
with open("cat_image_s3_urls.csv", "w", newline="") as csvfile:
  csv_writer = csv.writer(csvfile)
  csv_writer.writerow(["image_name", "breed", "s3_url"])  # Header row

  # Walk through the data directory and subfolders
  for root, folders, files in os.walk(data_dir):
    # Skip the data directory itself
    if root == data_dir:
      continue

    # Get the breed name from the subfolder name
    breed = os.path.basename(root)

    # Process each image in the subfolder
    for filename in files:
      image_path = os.path.join(root, filename)
      object_key = os.path.join(breed, filename)  # Include breed in S3 object key

      # Upload image and get URL
      image_url = upload_image_get_url(image_path, object_key)

      # Write information to CSV
      csv_writer.writerow([filename, breed, image_url])

print("Image URLs written to image_urls.csv")
