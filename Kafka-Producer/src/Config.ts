class Config {

    public kafkaHost: string;
    public kafkaPort: number;

    constructor() {
        console.log("Running with environment:", process.env.ENV);
        switch (process.env.ENV) {
            case "prod":
                this.kafkaHost = "3.225.207.252";
                this.kafkaPort = 9092;
                break;
            case "dev":
            default:
                this.kafkaHost = "3.225.207.252";
                this.kafkaPort = 9092;
        }
    }

}

export default new Config();
