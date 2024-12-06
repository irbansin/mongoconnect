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
        System.out.println(app.query1(1000));
        System.out.println(app.query2(32));
        System.out.println(app.query2Nest(32));
        System.out.println(app.query3());
        System.out.println(app.query3Nest());
        System.out.println(app.query4());
        System.out.println(app.query4Nest());
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

    public String query1(int custkey) {
        MongoCollection<Document> customerCol = db.getCollection("customer");
        Document customer = customerCol.find(eq("custkey", custkey)).first();
        return customer != null ? customer.getString("name") : null;
    }

    public String query2(int orderId) {
        MongoCollection<Document> ordersCol = db.getCollection("orders");
        Document order = ordersCol.find(eq("orderkey", orderId)).first();
        return order != null ? order.getString("orderdate") : null;
    }

    public String query2Nest(int orderId) {
        MongoCollection<Document> custOrdersCol = db.getCollection("custorders");

        Document custOrder = custOrdersCol.find(eq("orders.orderkey", orderId)).first();
        if (custOrder != null) {
            Object ordersObj = custOrder.get("orders");

            if (ordersObj instanceof List<?>) {
                @SuppressWarnings("unchecked")
                List<Document> orders = (List<Document>) ordersObj;

                for (Document order : orders) {
                    if (order.getInteger("orderkey") == orderId) {
                        return order.getString("orderdate");
                    }
                }
            }
        }
        return null;
    }

    public long query3() {
        MongoCollection<Document> ordersCol = db.getCollection("orders");
        return ordersCol.countDocuments();
    }

    public long query3Nest() {
        MongoCollection<Document> custOrdersCol = db.getCollection("custorders");
        long totalOrders = 0;

        for (Document doc : custOrdersCol.find()) {
            Object ordersObj = doc.get("orders");

            if (ordersObj instanceof List<?>) {
                @SuppressWarnings("unchecked")
                List<Document> orders = (List<Document>) ordersObj;
                totalOrders += orders.size(); // Add the size of the orders list
            }
        }

        return totalOrders;
    }

    public List<Document> query4() {
        MongoCollection<Document> ordersCol = db.getCollection("orders");
        MongoCollection<Document> customerCol = db.getCollection("customer");

        List<Document> results = new ArrayList<>();

        for (Document customer : customerCol.find()) {
            int custkey = customer.getInteger("custkey");

            List<Document> pipeline = new ArrayList<>();
            pipeline.add(new Document("$match", new Document("custkey", custkey)));
            pipeline.add(new Document("$group", new Document("_id", "$custkey")
                    .append("total", new Document("$sum", "$totalprice"))));

            AggregateIterable<Document> aggregation = ordersCol.aggregate(pipeline);
            Document aggResult = aggregation.first();
            double total = aggResult != null ? aggResult.getDouble("total") : 0.0;

            customer.append("totalOrderAmount", total);
            results.add(customer);
        }

        results.sort((d1, d2) -> Double.compare(d2.getDouble("totalOrderAmount"), d1.getDouble("totalOrderAmount")));

        return results.subList(0, Math.min(5, results.size()));
    }

    public List<Document> query4Nest() {
        MongoCollection<Document> custOrdersCol = db.getCollection("custorders");
        List<Document> results = new ArrayList<>();

        for (Document custOrder : custOrdersCol.find()) {
            Object ordersObj = custOrder.get("orders");
            double totalOrderAmount = 0.0;

            if (ordersObj instanceof List<?>) {
                @SuppressWarnings("unchecked")
                List<Document> orders = (List<Document>) ordersObj;

                for (Document order : orders) {
                    totalOrderAmount += order.getDouble("totalprice");
                }
            }

            custOrder.append("totalOrderAmount", totalOrderAmount);
            results.add(custOrder);
        }

        results.sort((d1, d2) -> Double.compare(d2.getDouble("totalOrderAmount"), d1.getDouble("totalOrderAmount")));

        return results.subList(0, Math.min(5, results.size()));
    }

}
