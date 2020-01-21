import Config from "./Config";
import KafkaFactory from "./KafkaFactory";
import MongoFactory from "./MongoFactory";
import readline = require("readline");

const kafkaFactoryObj = new KafkaFactory(Config.kafkaHost, Config.kafkaPort);

let topicNamesArray = new Array();

const consumer = kafkaFactoryObj.getKafkaConsumer(topicNamesArray);

let collectionObj = null;

const unsubscribedTopics = ["LinxupLocationTopic", "googleMap"];

const receivedTopicsSet = new Set();

// let timeoutId = null;

let lastReceivedTime = null;

let timeDiff = null;

let deviceCount;

const readlineInterface = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});

readlineInterface.question("Enter numbder of device for listening:", (numberOfDevice) => {
    deviceCount = parseInt(numberOfDevice, 10);
    readlineInterface.close();
    if (between(deviceCount, 1, 500)) {
        timeDiff = 3000;
    } else if (between(deviceCount, 501, 1000)) {
        timeDiff = 4000;
    } else if (between(deviceCount, 1001, 1500)) {
        timeDiff = 5000;
    } else {
        timeDiff = 7000;
    }
    // console.log("deviceCount:" + deviceCount + ", waitTime" + waitTime);
    startListening();
    /**
     * Check for new topics every 5 seconds
     */
    setInterval(() => {
        checkForNewTopicsAndSubscribe();
    }, 5 * 1000);

});

function between(x, min, max) {
    return x >= min && x <= max;
}

async function startListening() {
    consumer.on("message", (message) => {
        // console.log("Received message:", message.topic);
        // if (timeoutId) {
        //     clearTimeout(timeoutId);
        // }
        const currentTime = new Date().getTime();
        const diff = currentTime - lastReceivedTime;
        // console.log("diff:", diff);
        if (lastReceivedTime && ((diff) > timeDiff)) {
            console.log("Received messages ----- :", receivedTopicsSet.size);
            receivedTopicsSet.clear();
            // timeoutId = null;
        }
        lastReceivedTime = currentTime;
        receivedTopicsSet.add(message.topic);
        // timeoutId = setTimeout(() => {
        //     console.log("freceived topics-----:", receivedTopicsSet.size);
        //     receivedTopicsSet.clear();
        // }, 3000);
        const receivedData = JSON.parse(message.value);
        receivedData.receivedTime = currentTime;
        if (collectionObj == null) {
            new MongoFactory().openMongoConnection((deviceCount / 2)).then((mongoClient: any) => {
                collectionObj = mongoClient.collection(Config.dataCollectionName);
                collectionObj.insertMany([receivedData], (err, result) => {
                    if (err) {
                        console.log("insert error:", err);
                    }
                    // con sole.log("insert result", result);
                    // console.log("Inserted document into the collection");
                });
            });
        } else {
            collectionObj.insertMany([receivedData], (err, result) => {
                if (err) {
                    console.log("insert error:", err);
                }
                // console.log("insert result", result);
                // console.log("Inserted document into the collection");
            });
        }

    });
}

consumer.on("error", (err) => {
    console.log("err", err);
});

consumer.on("offsetOutOfRange", (err) => {
    console.log("err", err);
});

const admin = kafkaFactoryObj.getKafkaAdmin();
admin.listTopics((err, res) => {
    if (res && res[1]) {
        topicNamesArray = Object.keys(res[1].metadata);
        for (const topic of unsubscribedTopics) {
            topicNamesArray.splice(topicNamesArray.indexOf(topic), 1);
        }
        consumer.addTopics(topicNamesArray, (addErr, added) => {
            // console.log("Topic added to consumer");
        });
    }
});

async function checkForNewTopicsAndSubscribe() {
    console.log("Checking for new topics...");
    admin.listTopics((err, res) => {
        if (res && res[1]) {
            const topicsArray = Object.keys(res[1].metadata);
            for (const topic of unsubscribedTopics) {
                topicsArray.splice(topicsArray.indexOf(topic), 1);
            }

            const subscribedTopics = new Set(topicNamesArray);
            const newTopics = new Array();
            for (const topic of topicsArray) {
                if (!subscribedTopics.has(topic)) {
                    newTopics.push(topic);
                }
            }
            // console.log("new Topics found:", newTopics);
            if (newTopics.length) {
                topicNamesArray = topicsArray;
                consumer.addTopics(newTopics, (addErr, added) => {
                    console.log("Topic added to consumer:", added);
                    if (addErr) {
                        console.log("Error while adding new topic to consumer:", addErr);
                    }
                });
            }
        }
    });

}

new MongoFactory().openMongoConnection((deviceCount / 2)).then((mongoClient: any) => {
    collectionObj = mongoClient.collection(Config.dataCollectionName);
});
