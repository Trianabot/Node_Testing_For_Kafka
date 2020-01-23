import Config from "./Config";
import MongoFactory from "./MongoFactory";

let publishedCollectionObj = null;
let receivedCollectionObj = null;

let fetchLimit = 100;
let timeout = 1000;

const processedObjects = new Set();
const alreadyRead = new Set();
const tempStorageArray = new Array();
tempStorageArray.push(new Array());
let storageIndex = 0;

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
                timeout = 100;
                fetchLimit = 100;
            } else if (numOfDocs > 10000) {
                timeout = 150;
                fetchLimit = 100;
            } else if (numOfDocs > 7500) {
                timeout = 200;
                fetchLimit = 100;
            } else if (numOfDocs > 5000) {
                timeout = 300;
                fetchLimit = 125;
            } else if (numOfDocs > 1000) {
                timeout = 1000;
                fetchLimit = 200;
            }
        });
    }, 10000);

    setInterval(() => {
        const canDelete = new Array();
        for (const key of processedObjects) {
            canDelete.push(key);
            processedObjects.delete(key);
            alreadyRead.delete(key);
        }

        publishedCollectionObj.deleteMany({ uniqueKey: { $in: canDelete } },
            (deleteErr, result) => {
                if (deleteErr) {
                    console.log("deleteErr:", deleteErr);
                } else {
                    console.log("deleted from pub:", result.deletedCount);
                }

            });

        receivedCollectionObj.deleteMany({ processed: true },
            (deleteErr, result) => {
                if (deleteErr) {
                    console.log("deleteErr:", deleteErr);
                } else {
                    console.log("deleted from received:", result.deletedCount);
                }

            });

    }, 3000);

});

async function startValidaion() {
    console.log("fafaf");
    publishedCollectionObj.find({ uniqueKey: { $nin: Array.from(alreadyRead) } }).limit(fetchLimit)
        .toArray(async (err, docsArray) => {

            if (err) {
                console.log("Find error:", err);
            }
            let uKey = null;
            for (const doc of docsArray) {
                uKey = doc.uniqueKey;
                alreadyRead.add(uKey);

                if (tempStorageArray[storageIndex].length < 99) {
                    tempStorageArray[storageIndex].push(uKey);
                } else {
                    tempStorageArray[storageIndex].push(uKey);
                    const indexToSave = storageIndex;
                    for (let index = 0; index < tempStorageArray.length; index++) {
                        // console.log("in:" + index + ", len:" + messageStorageArr[index].length);
                        if (tempStorageArray[index].length === 0) {
                            storageIndex = index;
                            break;
                        }
                    }
                    if (indexToSave === storageIndex) {
                        tempStorageArray.push(new Array());
                        storageIndex = tempStorageArray.length - 1;
                    }
                    validateAndDelete(tempStorageArray[indexToSave], indexToSave);
                }

            }

            setTimeout(() => {
                startValidaion();
            }, timeout);

        });
}

async function validateAndDelete(uniqueKeys, index) {
    // let delay = -1;
    receivedCollectionObj.updateMany(
        { uniqueKey: { $in: uniqueKeys } },
        {
            $set: { processed: true },
        },
        (updateErr, updateResult) => {
            if (updateErr) {
                console.log("updateErr:", updateErr);
            } else {

                if (updateResult.modifiedCount === uniqueKeys.length) {
                    // console.log("Some documents not received, keys:" +
                    //     uniqueKeys.length + ", modified:" + updateResult.modifiedCount + ", docs:", uniqueKeys);
                    for (const key of uniqueKeys) {
                        processedObjects.add(key);
                    }
                } else {
                    console.log("uniqueKeys:", uniqueKeys);
                    console.log("not found", updateResult.modifiedCount + "-" + uniqueKeys.length);
                    for (const key of uniqueKeys) {
                        alreadyRead.delete(key);
                    }
                }
            }
            tempStorageArray[index] = new Array();

        });
}
