import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StoreData {
    /**
     * Parses a JSON file and creates a Store object
     * @param jsonFilePath Path to the JSON file
     * @return Store object created from the JSON data
     */
    public static Store parseStoreJson(String jsonFilePath) throws IOException, ParseException {
        // Initialize JSON Parser
        JSONParser parser = new JSONParser();

        // Read and parse the JSON file
        try (FileReader reader = new FileReader(jsonFilePath)) {
            JSONObject jsonObject = (JSONObject) parser.parse(reader);

            // Extract Store details
            String storeName = (String) jsonObject.get("StoreName");
            double latitude = ((Number) jsonObject.get("Latitude")).doubleValue();
            double longitude = ((Number) jsonObject.get("Longitude")).doubleValue();
            String foodCategory = (String) jsonObject.get("FoodCategory");
            int stars = ((Number) jsonObject.get("Stars")).intValue();
            int noOfVotes = ((Number) jsonObject.get("NoOfVotes")).intValue();
            String storeLogoPath = (String) jsonObject.get("StoreLogo");

            // Extract Product details
            JSONArray productArray = (JSONArray) jsonObject.get("Products");
            ArrayList<Product> products = new ArrayList<>();
            for (Object productObj : productArray) {
                JSONObject productJson = (JSONObject) productObj;

                String productName = (String) productJson.get("ProductName");
                String productType = (String) productJson.get("ProductType");
                // Fix: Correct field name for Available Amount
                int availableAmount = ((Number) productJson.get("Available Amount")).intValue();
                double price = ((Number) productJson.get("Price")).doubleValue();

                Product product = new Product(productName, productType, availableAmount, price);
                products.add(product);
            }

            // Create and return the Store object
            return new Store(storeName, latitude, longitude, foodCategory, stars, noOfVotes, storeLogoPath, products);
        }
    }

    /**
     * Parses a JSON array and creates a list of Store objects
     * @param jsonFilePath Path to the JSON file containing an array of stores
     * @return List of Store objects created from the JSON data
     */
    public static ArrayList<Store> parseStoresJsonArray(String jsonFilePath) throws IOException, ParseException {
        ArrayList<Store> stores = new ArrayList<>();
        JSONParser parser = new JSONParser();

        try (FileReader reader = new FileReader(jsonFilePath)) {
            JSONArray jsonArray = (JSONArray) parser.parse(reader);

            for (Object obj : jsonArray) {
                JSONObject jsonObject = (JSONObject) obj;

                String storeName = (String) jsonObject.get("StoreName");
                double latitude = ((Number) jsonObject.get("Latitude")).doubleValue();
                double longitude = ((Number) jsonObject.get("Longitude")).doubleValue();
                String foodCategory = (String) jsonObject.get("FoodCategory");
                int stars = ((Number) jsonObject.get("Stars")).intValue();
                int noOfVotes = ((Number) jsonObject.get("NoOfVotes")).intValue();
                String storeLogoPath = (String) jsonObject.get("StoreLogo");

                // Extract Product details
                JSONArray productsArray = (JSONArray) jsonObject.get("Products");
                ArrayList<Product> products = new ArrayList<>();
                
                for (Object prodObj : productsArray) {
                    JSONObject productJson = (JSONObject) prodObj;

                    String productName = (String) productJson.get("ProductName");
                    String productType = (String) productJson.get("ProductType");
                    // Fix: Correct field name for Available Amount
                    int availableAmount = ((Number) productJson.get("Available Amount")).intValue();
                    double price = ((Number) productJson.get("Price")).doubleValue();

                    products.add(new Product(productName, productType, availableAmount, price));
                }

                Store store = new Store(storeName, latitude, longitude, foodCategory, stars, noOfVotes, storeLogoPath, products);
                stores.add(store);
            }
        }
        
        return stores;
    }

    /**
     * Calculates the price category of a store based on the average price of its products
     * @param store The store to calculate the price category for
     * @return Price category as a string ("$", "$$", or "$$$")
     */
    public static String calculatePriceCategory(Store store) {
        return calculatePriceCategory(store.getProducts());
    }

    /**
     * Calculates the price category based on a list of products
     * @param products List of products to calculate the average price from
     * @return Price category as a string ("$", "$$", or "$$$")
     */
    public static String calculatePriceCategory(List<Product> products) {
        if (products.isEmpty()) {
            return "$"; // Default category for stores with no products
        }
        
        double totalPrice = 0;
        for (Product product : products) {
            totalPrice += product.getPrice();
        }
        double averagePrice = totalPrice / products.size();

        if (averagePrice <= 5) {
            return "$";
        } else if (averagePrice <= 15) {
            return "$$";
        } else {
            return "$$$";
        }
    }
}
