/**
 * This is the source code to be executed by AWS Lambda.
 * This program registers data in CSV files stored in AWS S3 to AWS DynamoDB. 
 * Records = [
 *   {
 *      "s3": {
 *           "bucket": {
 *               "name": ""
 *           },
 *           "object": {
 *               "key": ""
 *           }
 *       },
 *       "dynamoDB": {
 *           "TableName": ""
 *       }
 *   }
 * ]
 */

'use strict'
const AWS = require('aws-sdk');
const fs = require('fs');
AWS.config.update({ region: 'ap-northeast-1' });
const dynamoDB = new AWS.DynamoDB();
const S3 = new AWS.S3();

const sleep = function (ms) {
    return new Promise(resolve => setTimeout(() => resolve(), ms));
};

exports.handler = (event, context, callback) => {
    try {
        let Records;
        if (event.Records) {
            Records = event.Records;
        } else if (fs.existsSync('./Records.json')) {
            Records = require('./Records.json');
        }
        if (!Records) {
            throw new Error('Specify the target information as Records in the event.The format of the Records should be as follows: \n{ "Records": [ { "s3": { "bucket": { "name": "gift-private" }, "object": { "key": "test/gift_url_recipient.csv" } }, "dynamoDB": { "TableName": "gift-test-recipient-ticket" } } ] }.');
        }
        Records.forEach((record) => {
            let srcBucket = record.s3.bucket.name;
            let srcKey = record.s3.object.key;
            const tblName = record.dynamoDB.TableName;
            console.log(`event: ${JSON.stringify(event)}`);
            console.log(`srcBucket: ${srcBucket}`);
            console.log(`srcKey: ${srcKey}`);
            console.log(`tblName: ${tblName}`);
            S3.getObject({
                Bucket: srcBucket,
                Key: srcKey,
            }, function (err, s3obj) {
                if (err !== null) {
                    return callback(err, null);
                }
                let fileData = s3obj.Body.toString('utf-8');
                let rows = fileData.split('\r\n');
                if (rows.length < 1) {
                    throw new Error('The target CSV file must have at least two lines. The first line is the header.');
                }
                rows.forEach((line) => {
                    line = line.replace(/\s/, ''); //delete all blanks
                    // console.log(`line: ${line}`);
                });
                let headers = rows[0].split(',');
                console.log(`headers: ${headers}`);
                const data = rows.slice(1);
                console.log(`data length: ${data.length}`);

                // Using forEach may cause a tight update of dynamoDB, so use for to execute one line at a time.
                for (let i = 0; i < data.length; i++) {
                    const line = data[i];
                    if (line == '') {
                        return;
                    }
                    // console.log(`line: ${line}`);
                    let obj = {};
                    let items = line.split(',');
                    for (let j = 0; j < items.length; j++) {
                        obj[headers[j].toString()] = {
                            S: items[j]
                        };
                        // console.log('adding ' + JSON.stringify(obj));
                        const params = {
                            Item: obj,
                            TableName: tblName
                        };
                        sleep(100)
                            .then(() => {
                                dynamoDB.putItem(params, (err,) => {
                                    if (err) {
                                        console.log("Insert Error: \n" + err);
                                    } else {
                                        console.log(err);
                                    }
                                });
                            });
                    }
                }
            });
        });
        callback(null, `successed.`);
    } catch (err) {
        console.log(`err: ${err}`);
        callback(err);
    }
};
