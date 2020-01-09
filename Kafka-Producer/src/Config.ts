class Config {

    public kafkaHost: string;
    public kafkaPort: number;

    public mongoUrl: string;
    public mongoDatabaseName: string;

    public dataCollectionName: string = "kafka_data";

    constructor() {
        let mongoHost: string;
        let mongoPort: number;
        console.log("Running with environment:", process.env.ENV);
        switch (process.env.ENV) {
            case "prod":
                this.kafkaHost = "3.225.207.252";
                this.kafkaPort = 9092;
                mongoHost = "127.0.0.1";
                mongoPort = 25015;
                this.mongoDatabaseName = "Temp";
                break;
            case "dev":
            default:
                this.kafkaHost = "3.225.207.252";
                this.kafkaPort = 9092;
                mongoHost = "127.0.0.1";
                mongoPort = 25015;
                this.mongoDatabaseName = "Temp";
        }

        this.mongoUrl = "mongodb://" + mongoHost + ":" + mongoPort + "/" + this.mongoDatabaseName + "?retryWrites=true&w=majority";
    }

}

export default new Config();
