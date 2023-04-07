# S3 CSV data to DybamoDB
This is a program run by AWS Lambda that registers data in a CSV file stored in S3 to DynamoDB.

# How to use
1. Upload or copy-paset `index.js` to AWS Lambda.
2. Correct the bucket name and key of the CSV file on S3 in `Records.json`.
3. Correct the name of a table in DynamoDB in `Records.json`.
3. Uplpad `Records.json` to AWS Lambda.
4. Run the Lambda.