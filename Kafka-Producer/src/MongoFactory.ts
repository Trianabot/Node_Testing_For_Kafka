import Config from "./Config";

class MongoFactory {

    public mongoClient: any;

    constructor() {
        this.openMongoConnection();
    }

    private async openMongoConnection() {
        require("mongodb").MongoClient.connect(Config.mongoUrl,
            { poolSize: 40, auto_reconnect: true, useUnifiedTopology: true },
            (error, client) => {
                if (error) {
                    console.error("Error during connection to mongo server", error);
                    throw new Error(error);
                }
                this.mongoClient = client.db(Config.mongoDatabaseName);
            });
    }

}

export default new MongoFactory();
