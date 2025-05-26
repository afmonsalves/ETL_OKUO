# ETL Okuo DS Assessment

Extraction, Transformation, and Loading (ETL) project. Intended to collect sales data from October 1st, 2014 to December 28, 2014 from an anonymous retailer and obtain insights about its recurrent customers and their most probable products.

## Basic information

For an input dataset stored in an Amazon Simple Storage Web Service, you will need to have in a .env file:
- aws_access_key_id
- aws_secret_access_key
- s3_bucket
- input_path (object key for the input data)
- output_path (object key for the output data)

## How to run

- **Using Linux command line**: *uv run python src/ETL_OKUO/main.py*
- It is also possible to execute the process using the **Lambda-Docker image**; for that, you will have to create and run the Docker image locally.

## output

You will have a clean dataset in AWS-s3 that includes, for each recurrent user:
- The three products that are more frequently bought
- The day of the week the user is most likely to go to the store
- Number of times the client has bought that specific item
- Probability of buying that product in a trip to the store (calculated as a percentage of the total products bought by that client)

(Header example of the clean dataset)
![image](https://github.com/user-attachments/assets/e9b640ae-8f70-499d-abe4-c833410a46a4)
