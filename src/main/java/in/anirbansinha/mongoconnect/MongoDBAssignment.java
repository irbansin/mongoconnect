package in.anirbansinha.mongoconnect;

import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.*;
import org.bson.Document;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import io.github.cdimascio.dotenv.Dotenv;



public class MongoDBAssignment {
    Dotenv dotenv = Dotenv.load();
    public static final String DATABASE_NAME = "assignment7-g23ai2084";
    public MongoClient mongoClient;
    public MongoDatabase db;

    public static void main(String[] args) throws Exception {
        MongoDBAssignment app = new MongoDBAssignment();
        app.connect();
        app.load();
        app.loadNest();
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

    public void load() throws Exception {
        MongoCollection<Document> customerCol = db.getCollection("customer");
        MongoCollection<Document> ordersCol = db.getCollection("orders");
        
        // Clear existing data
        customerCol.drop();
        ordersCol.drop();
        
        List<Document> customers = new ArrayList<>();
        List<Document> orders = new ArrayList<>();
    
        // Load customer data
        try (BufferedReader customerReader = new BufferedReader(new FileReader("data/customer.tbl"))) {
            String line;
            while ((line = customerReader.readLine()) != null) {
                String[] fields = line.split("\\|"); // TPC-H uses "|" as a delimiter
                if (fields.length >= 4) { // Ensure there are enough fields
                    Document customer = new Document("custkey", Integer.parseInt(fields[0]))
                            .append("name", fields[1])
                            .append("address", fields[2])
                            .append("nationkey", Integer.parseInt(fields[3]));
                    customers.add(customer);
                }
            }
        }
    
        // Load order data
        try (BufferedReader orderReader = new BufferedReader(new FileReader("data/order.tbl"))) {
            String line;
            while ((line = orderReader.readLine()) != null) {
                String[] fields = line.split("\\|"); // TPC-H uses "|" as a delimiter
                if (fields.length >= 4) { // Ensure there are enough fields
                    Document order = new Document("orderkey", Integer.parseInt(fields[0]))
                            .append("custkey", Integer.parseInt(fields[1]))
                            .append("orderdate", fields[2])
                            .append("totalprice", Double.parseDouble(fields[3]));
                    orders.add(order);
                }
            }
        }
    
        // Insert data into MongoDB
        if (!customers.isEmpty()) {
            customerCol.insertMany(customers);
        }
        if (!orders.isEmpty()) {
            ordersCol.insertMany(orders);
        }
        
        System.out.println("Data loaded successfully into MongoDB.");
    }
    
    /**
        * Loads customer and orders TPC-H data into a single collection.
    */
    public void loadNest() throws Exception {
        MongoCollection<Document> custOrdersCol = db.getCollection("custorders");
        custOrdersCol.drop();

        MongoCollection<Document> customerCol = db.getCollection("customer");
        MongoCollection<Document> ordersCol = db.getCollection("orders");

        for (Document customer : customerCol.find()) {
            List<Document> orders = ordersCol.find(eq("custkey", customer.getInteger("custkey")))
                    .into(new ArrayList<>());
            customer.append("orders", orders);
            custOrdersCol.insertOne(customer);
        }
    }

}
