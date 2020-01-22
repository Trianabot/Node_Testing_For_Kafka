import Config from "./Config";
import MongoFactory from "./MongoFactory";

let publishedCollectionObj = null;
let receivedCollectionObj = null;

let deletedCount = 0;

new MongoFactory().openMongoConnection(10).then((mongoClient: any) => {

    publishedCollectionObj = mongoClient.collection(Config.publishedCollectionName);
    receivedCollectionObj = mongoClient.collection(Config.receivedCollectionName);

    startValidaion();

});

async function startValidaion() {
    deletedCount = 0;
    publishedCollectionObj.find({}).toArray(async (err, docsArray) => {
        if (err) {
            console.log("Find error:", err);
        }
        const arrLength = docsArray.length;
        console.log("pub docs:", arrLength);

        for (let index = 0; index < arrLength; index++) {
            validateAndDelete(docsArray[index]);
        }

        setTimeout(() => {
            console.log("deletedCount:", deletedCount);
            startValidaion();
        }, 10000);

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
                            publishedCollectionObj.deleteOne({ _id: message._id }, (pubDeleteErr, pubResult) => {
                                if (pubDeleteErr) {
                                    console.log("pubDeleteErr: ", pubDeleteErr);
                                } else {
                                    deletedCount++;
                                }
                                // console.log("Deleted published");
                            });
                        }
                        // console.log("deleted received");
                    });

                } else {
                    console.log("Long delay:", delay);
                }
                // console.log("delay: ", delay);
                // console.log("find:", documents);
            }
            if (documents.length > 1) {
                console.log("dupe records found");
            }
            // else {
            //     console.log("DOcument not found: ", {
            //         deviceId: message.deviceId,
            //         publishedTime: message.publishedTime,
            //     });
            //     console.log("index:", index);
            // }

        });
}
