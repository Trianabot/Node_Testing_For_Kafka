import Config from "./Config";
import MongoFactory from "./MongoFactory";

let publishedCollectionObj = null;
let receivedCollectionObj = null;

let deletedCount = 0;
let fetchLimit = 100;
let timeout = 1000;

new MongoFactory().openMongoConnection(10).then((mongoClient: any) => {

    publishedCollectionObj = mongoClient.collection(Config.publishedCollectionName);
    receivedCollectionObj = mongoClient.collection(Config.receivedCollectionName);

    startValidaion();

    setInterval(() => {
        publishedCollectionObj.estimatedDocumentCount({}, (error, numOfDocs) => {
            console.log("Total documents:" + numOfDocs + ", timeout:" + timeout + ", fetchLimit:" + fetchLimit);
            timeout = 10000;
            if (error) {
                console.log("Count error:", error);
            }
            if (numOfDocs > 50000) {
                timeout = 10;
                fetchLimit = 25;
            } else if (numOfDocs > 10000) {
                timeout = 100;
                fetchLimit = 50;
            } else if (numOfDocs > 7500) {
                timeout = 200;
                fetchLimit = 70;
            } else if (numOfDocs > 5000) {
                timeout = 300;
                fetchLimit = 100;
            } else if (numOfDocs > 1000) {
                timeout = 1000;
                fetchLimit = 200;
            }
        });
    }, 10000);

});

async function startValidaion() {
    deletedCount = 0;
    publishedCollectionObj.find({ processed: false }).sort({ _id: 1 }).limit(fetchLimit)
        .toArray(async (err, docsArray) => {
            if (err) {
                console.log("Find error:", err);
            }
            const arrLength = docsArray.length;
            const idArray = new Array();
            for (const doc of docsArray) {
                idArray.push(doc._id);
            }
            publishedCollectionObj.updateMany({ _id: { $in: idArray } },
                { $set: { processed: true } }, (updateErr, doc) => {
                    if (updateErr) {
                        console.log("updateErr:", err);
                    } else {
                        for (let index = 0; index < arrLength; index++) {
                            validateAndDelete(docsArray[index]);
                        }
                    }
                    setTimeout(() => {
                        console.log("deletedCount:", deletedCount);
                        console.log("date:", new Date());
                        startValidaion();
                    }, timeout);
                });
        });
}

async function validateAndDelete(message) {
    let delay = -1;
    receivedCollectionObj.find
        ({ deviceId: message.deviceId, publishedTime: message.publishedTime }).toArray((findErr, documents) => {
            if (documents.length === 1) {
                delay = documents[0].receivedTime - message.publishedTime;
                if (delay <= 5000 || true) {
                    receivedCollectionObj.deleteOne({ _id: documents[0]._id }, (deleteErr, result) => {
                        if (deleteErr) {
                            console.log("deleteErr: ", deleteErr);
                        } else {
                            // console.log("deleted received");
                            publishedCollectionObj.deleteOne({ _id: message._id }, (pubDeleteErr, pubResult) => {
                                if (pubDeleteErr) {
                                    console.log("pubDeleteErr: ", pubDeleteErr);
                                } else {
                                    // console.log("deleted published");
                                    deletedCount++;
                                }
                            });
                        }
                    });

                } else {
                    console.log("Long delay:", delay);
                }
                // console.log("delay: ", delay);
                // console.log("find:", documents);
            } else if (documents.length > 1) {
                console.log("dupe records found");
            } else {
                console.log("DOcument not found: ", {
                    deviceId: message.deviceId,
                    publishedTime: message.publishedTime,
                });
            }

        });
}
