import weaviate
from weaviate.util import generate_uuid5

import os
import base64

client = weaviate.Client("http://localhost:8080/")
data_dir = "./images"
b64_dir = "./base64_images/"

def createSchema():
    print("Creating schema...")
    schema = {
      "classes": [
        {
          "class": "Fruit",
          "moduleConfig": {
               "img2vec-neural": {
                   "imageFields": [
                       "image"
                   ]
               }
           },
          "vectorIndexType": "hnsw",
          "vectorizer": "img2vec-neural",
          "properties": [
            {
              "name": "file_name",
              "dataType": ["text"]
            },
            {
              "name": "image",
              "dataType": ["blob"]
            }
          ]
        }
      ]
    }

    try:
        client.schema.create(schema)
        print("Finished creating schema")

    except:
        print("Already contains schema!")
    

def imageToB64():
    print("Converting images to base64...")
    if not os.path.exists(b64_dir):
        os.makedirs(b64_dir)

    for filename in os.listdir(data_dir):
        if filename.endswith(".jpg"):
            input_file_path = os.path.join(data_dir, filename)
            output_file_path = os.path.join(b64_dir, filename.replace(".png", ".b64"))

            with open(input_file_path, "rb") as img_file:
                img_data = img_file.read()
                img_base64 = base64.b64encode(img_data).decode("utf-8")

            with open(output_file_path, "w") as b64_file:
                b64_file.write(img_base64)
                
    print("Finished converting")

            
def set_up_batch():
    print("Setting up batch...")
    client.batch.configure(batch_size=100, dynamic=True, timeout_retries=3, callback=None)
    print("Finished setting up batch")
    
def import_data():
    print("Importing Data...")
    with client.batch as batch:
        for encoded_file_path in os.listdir(b64_dir):
            with open(b64_dir + encoded_file_path) as file:
                file_lines = file.readlines()

                base64_encoding = " ".join(file_lines)
                base64_encoding = base64_encoding.replace("\n", "").replace(" ", "")

                file_name = encoded_file_path.replace(".b64", "")

                data_properties = {
                    "file_name": file_name,
                    "image": base64_encoding
                }

                batch.add_data_object(data_properties, "Fruit")
                
    print("Finished importing data")

def batch():
    imageToB64()
    set_up_batch()
    import_data()

def query(query):
    testImage = {}

    with open(query, "rb") as test:
        img_data = test.read()
        img_base64 = base64.b64encode(img_data).decode("utf-8")
        
        testImage["image"] = img_base64

    query_result = client.query\
        .get("Fruit", ["file_name"])\
        .with_near_image(testImage, encode=False)\
        .with_limit(3)\
        .do()

    print(query_result)
    
def main():
    createSchema()
    batch()
    query("test.jpg")
    
if __name__ == "__main__":
    main()