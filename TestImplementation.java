import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Κλάση για τον έλεγχο της υλοποίησης του backend
 * Δημιουργεί ένα απλό σενάριο δοκιμής για να επιβεβαιώσει ότι οι αλλαγές λειτουργούν σωστά
 */
public class TestImplementation {
    public static void main(String[] args) {
        System.out.println("=== Έναρξη δοκιμής υλοποίησης ===");
        
        // Δοκιμή 1: Έλεγχος υπολογισμού κατηγορίας ακρίβειας
        testPriceCategory();
        
        // Δοκιμή 2: Έλεγχος διαχείρισης JSON
        testJsonParsing();
        
        // Δοκιμή 3: Έλεγχος MapReduce
        testMapReduce();
        
        System.out.println("=== Ολοκλήρωση δοκιμής υλοποίησης ===");
    }
    
    /**
     * Δοκιμή υπολογισμού κατηγορίας ακρίβειας
     */
    private static void testPriceCategory() {
        System.out.println("\n--- Δοκιμή υπολογισμού κατηγορίας ακρίβειας ---");
        
        // Δημιουργία προϊόντων για δοκιμή
        ArrayList<Product> products1 = new ArrayList<>();
        products1.add(new Product("Product1", "Type1", 10, 3.0));
        products1.add(new Product("Product2", "Type1", 10, 4.0));
        products1.add(new Product("Product3", "Type2", 10, 5.0));
        
        ArrayList<Product> products2 = new ArrayList<>();
        products2.add(new Product("Product4", "Type1", 10, 10.0));
        products2.add(new Product("Product5", "Type2", 10, 12.0));
        products2.add(new Product("Product6", "Type3", 10, 14.0));
        
        ArrayList<Product> products3 = new ArrayList<>();
        products3.add(new Product("Product7", "Type1", 10, 16.0));
        products3.add(new Product("Product8", "Type2", 10, 18.0));
        products3.add(new Product("Product9", "Type3", 10, 20.0));
        
        // Δημιουργία καταστημάτων
        Store store1 = new Store("Store1", 37.9, 23.7, "pizzeria", 4, 10, "logo1.png", products1);
        Store store2 = new Store("Store2", 37.8, 23.8, "burger", 3, 5, "logo2.png", products2);
        Store store3 = new Store("Store3", 37.7, 23.9, "sushi", 5, 20, "logo3.png", products3);
        
        // Έλεγχος κατηγορίας ακρίβειας
        System.out.println("Store1 price category: " + store1.calculatePriceCategory() + " (expected: $)");
        System.out.println("Store2 price category: " + store2.calculatePriceCategory() + " (expected: $$)");
        System.out.println("Store3 price category: " + store3.calculatePriceCategory() + " (expected: $$$)");
        
        // Έλεγχος κατηγορίας ακρίβειας με κενή λίστα προϊόντων
        Store emptyStore = new Store("EmptyStore", 37.6, 24.0, "cafe", 3, 2, "logo4.png", new ArrayList<>());
        System.out.println("Empty store price category: " + emptyStore.calculatePriceCategory() + " (expected: $)");
        
        // Έλεγχος μέσω StoreData
        System.out.println("Store1 price category via StoreData: " + StoreData.calculatePriceCategory(store1) + " (expected: $)");
    }
    
    /**
     * Δοκιμή διαχείρισης JSON
     */
    private static void testJsonParsing() {
        System.out.println("\n--- Δοκιμή διαχείρισης JSON ---");
        
        // Δημιουργία δοκιμαστικού JSON
        String jsonContent = "[\n" +
            "  {\n" +
            "    \"StoreName\": \"TestPizzeria\",\n" +
            "    \"Latitude\": 37.9932963,\n" +
            "    \"Longitude\": 23.733413,\n" +
            "    \"FoodCategory\": \"pizzeria\",\n" +
            "    \"Stars\": 4,\n" +
            "    \"NoOfVotes\": 15,\n" +
            "    \"StoreLogo\": \"/usr/bin/images/testLogo.png\",\n" +
            "    \"Products\": [\n" +
            "      {\n" +
            "        \"ProductName\": \"margarita\",\n" +
            "        \"ProductType\": \"pizza\",\n" +
            "        \"Available Amount\": 50,\n" +
            "        \"Price\": 9.2\n" +
            "      },\n" +
            "      {\n" +
            "        \"ProductName\": \"special\",\n" +
            "        \"ProductType\": \"pizza\",\n" +
            "        \"Available Amount\": 30,\n" +
            "        \"Price\": 12.0\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "]";
        
        try {
            // Αποθήκευση του JSON σε προσωρινό αρχείο
            File tempFile = File.createTempFile("test_store", ".json");
            try (FileWriter writer = new FileWriter(tempFile)) {
                writer.write(jsonContent);
            }
            
            System.out.println("Δημιουργήθηκε προσωρινό αρχείο JSON: " + tempFile.getAbsolutePath());
            
            // Δοκιμή ανάγνωσης του JSON
            try {
                ArrayList<Store> stores = StoreData.parseStoresJsonArray(tempFile.getAbsolutePath());
                System.out.println("Επιτυχής ανάγνωση JSON. Αριθμός καταστημάτων: " + stores.size());
                
                if (!stores.isEmpty()) {
                    Store store = stores.get(0);
                    System.out.println("Όνομα καταστήματος: " + store.getStoreName());
                    System.out.println("Αριθμός προϊόντων: " + store.getProducts().size());
                    System.out.println("Κατηγορία ακρίβειας: " + store.calculatePriceCategory());
                }
            } catch (Exception e) {
                System.out.println("Σφάλμα κατά την ανάγνωση του JSON: " + e.getMessage());
                e.printStackTrace();
            }
            
            // Διαγραφή του προσωρινού αρχείου
            tempFile.delete();
            
        } catch (IOException e) {
            System.out.println("Σφάλμα κατά τη δημιουργία του προσωρινού αρχείου: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Δοκιμή λειτουργίας MapReduce
     */
    private static void testMapReduce() {
        System.out.println("\n--- Δοκιμή λειτουργίας MapReduce ---");
        
        // Δημιουργία δοκιμαστικών workers
        String[][] testWorkers = {
            {"localhost", "8001"},
            {"localhost", "8002"}
        };
        
        // Δημιουργία αντικειμένου MapReduceImplementation
        MapReduceImplementation mapReduce = new MapReduceImplementation(testWorkers);
        
        // Δημιουργία δοκιμαστικού αιτήματος
        MapReduceRequest request = new MapReduceRequest(
            37.98, // latitude
            23.72, // longitude
            new ArrayList<>(Arrays.asList("pizzeria", "burger")), // food categories
            3,     // minimum stars
            "$$",  // price category
            5.0    // radius in km
        );
        
        System.out.println("Δημιουργήθηκε δοκιμαστικό αίτημα MapReduce:");
        System.out.println(request);
        
        // Δοκιμή συγκέντρωσης αποτελεσμάτων
        Map<String, Integer> testResults = new HashMap<>();
        testResults.put("Store1", 10);
        testResults.put("Store2", 15);
        
        Map<String, Integer> testResults2 = new HashMap<>();
        testResults2.put("Store2", 5);
        testResults2.put("Store3", 20);
        
        ArrayList<Map<String, Integer>> partialResults = new ArrayList<>();
        partialResults.add(testResults);
        partialResults.add(testResults2);
        
        // Δοκιμή μεθόδου reduceAggregationResults μέσω reflection
        try {
            java.lang.reflect.Method method = MapReduceImplementation.class.getDeclaredMethod(
                "reduceAggregationResults", ArrayList.class);
            method.setAccessible(true);
            
            @SuppressWarnings("unchecked")
            Map<String, Integer> finalResult = (Map<String, Integer>) method.invoke(mapReduce, partialResults);
            
            System.out.println("Αποτελέσματα συγκέντρωσης:");
            for (Map.Entry<String, Integer> entry : finalResult.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }
            
        } catch (Exception e) {
            System.out.println("Σφάλμα κατά τη δοκιμή της μεθόδου reduceAggregationResults: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
