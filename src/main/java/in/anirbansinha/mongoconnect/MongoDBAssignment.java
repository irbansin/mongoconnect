package in.anirbansinha.mongoconnect;

import com.mongodb.client.*;

import io.github.cdimascio.dotenv.Dotenv;



public class MongoDBAssignment {
    Dotenv dotenv = Dotenv.load();
    public static final String DATABASE_NAME = "sample_mflix";
    public MongoClient mongoClient;
    public MongoDatabase db;

    public static void main(String[] args) throws Exception {
        MongoDBAssignment app = new MongoDBAssignment();
        app.connect();

    }

    public MongoDatabase connect() {
        try {
            String connectionString =  dotenv.get("CONNECTION_STRING");
            mongoClient = MongoClients.create(connectionString);
            db = mongoClient.getDatabase(DATABASE_NAME);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return db;
    }



}
