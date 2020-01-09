import Config from "./Config";
import KafkaFactory from "./KafkaFactory";
import readline = require("readline");

const kafkaFactoryObj = new KafkaFactory(Config.kafkaHost, Config.kafkaPort);

let topicNamesArray = new Array();

const admin = kafkaFactoryObj.getKafkaAdmin();

const producer = kafkaFactoryObj.getKafkaProducer();

let producerReady = false;

let numberOfDevice = 1;

let devicePrefix = "DV_";

const deviceName = "Test_Device_";

let timeDelay = 10000;

const deviceIdArray = new Array();

// "setDuration" : 0,
// "remainDuration" : 0,
// "errorIndication" : 255,
// "receivedTime" : "1564632631019",
// "timeStamp" : 1564632631,
// "createdTime" : 1564634442,
// "deviceId" : "0000000000000HAVELLSFANTEST340"

const dataPacketObj = {
    currentSpeed: 3, currentState: 1, deviceId: null, humidity: 14, mode: 1, pktStatus: 0, pktTyp: 8481,
    seqNum: 8084, temperature: 23, uuid: null,
};

let readlineInterface = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});

readlineInterface.question("Do you want to customize the input(Y/N)? ", (customizeInput) => {
    if (customizeInput === "Y" || customizeInput === "y") {
        readlineInterface.close();
        readlineInterface = readline.createInterface({
            input: process.stdin,
            output: process.stdout,
        });
        readlineInterface.question("Enter Number of device for simulation:", (deviceNumber) => {
            if (deviceNumber) {
                numberOfDevice = parseInt(deviceNumber, 10);
            }
            readlineInterface.close();
            readlineInterface = readline.createInterface({
                input: process.stdin,
                output: process.stdout,
            });
            readlineInterface.question("Enter device prefix string:", (prefixString) => {
                if (prefixString) {
                    devicePrefix = prefixString + "_";
                }
                readlineInterface.close();
                readlineInterface = readline.createInterface({
                    input: process.stdin,
                    output: process.stdout,
                });
                readlineInterface.question("Enter time delay for publishing (In seconds):", (timeDelayInSeconds) => {
                    if (timeDelayInSeconds) {
                        timeDelay = parseInt(timeDelayInSeconds, 10) * 1000;
                    }
                    readlineInterface.close();
                    if (topicNamesArray.length) {
                        startProcess();
                    } else {
                        getTopics();
                        console.error("No topics are fetched from admin, waiting for 5 seconds");
                        setTimeout(() => {
                            startProcess();
                        }, 5000);
                    }

                });
            });
        });
    } else {
        readlineInterface.close();
        startProcess();
    }

});

async function getTopics() {
    admin.listTopics((err, res) => {
        if (res && res[1]) {
            topicNamesArray = Object.keys(res[1].metadata);
        } else {
            getTopics();
        }
    });
}

getTopics();

async function startProcess() {
    for (let index = 1; index <= numberOfDevice; index++) {
        deviceIdArray.push(devicePrefix + deviceName + index);
    }
    if (producerReady) {
        console.log("Starting the process with repeat interval of " + (timeDelay / 1000) + " seconds");
        startPublishing();
    } else {
        console.error("Producer is not ready yet.");
    }
}

producer.on("ready", () => {
    producerReady = true;
});

producer.on("error", (err) => {
    console.error("error while creating producer:", err);
});

async function startPublishing() {
    console.log("Publishing to Kafka");
    for (const deviceId of deviceIdArray) {
        dataPacketObj.deviceId = deviceId;
        dataPacketObj.uuid = deviceId;
        if (!topicNamesArray.find((topicName) => topicName === deviceId)) {
            console.log("topic not present:", deviceId);
            createTopicAndPublishData(deviceId, publishDataToTopic, dataPacketObj);
        } else {
            publishDataToTopic(deviceId, dataPacketObj);
        }
    }

    if (timeDelay) {
        setTimeout(() => {
            startPublishing();
        }, timeDelay);
    }

}

async function createTopicAndPublishData(topicName, callbackFunction, dataTosend) {
    console.log("Creating topic:", topicName);
    admin.createTopics([{
        partitions: 1,
        replicationFactor: 1,
        topic: topicName,
    }], (err, res) => {
        topicNamesArray.push(topicName);
        console.log("create topics", res);
        if (err) {
            console.error("Error occured while creating topic:", err);
        }
        callbackFunction(topicName, dataTosend);
    });
}

async function publishDataToTopic(topicName: string, dataTosend: object) {
    const payloads = [
        { topic: topicName, messages: JSON.stringify(dataTosend), partition: 0 },
    ];
    producer.send(payloads, (err, data) => {
        // console.log("sent:", data);
        if (err) {
            console.error("Error occured while publishing to topic:", err);
        }
    });
}
