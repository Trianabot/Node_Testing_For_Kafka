import Config from "./Config";

class MongoFactory {

    public mongoClient: any;

    public async openMongoConnection(maxPoolSize) {
        return new Promise((resolve, reject) => {
            require("mongodb").MongoClient.connect(Config.mongoUrl,
                { poolSize: maxPoolSize, auto_reconnect: true, useUnifiedTopology: true },
                (error, client) => {
                    if (error) {
                        console.error("Error during connection to mongo server", error);
                        throw new Error(error);
                    }
                    this.mongoClient = client.db(Config.mongoDatabaseName);
                    resolve(this.mongoClient);
                });
        });
    }
}

export default MongoFactory;
