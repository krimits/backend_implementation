import java.io.*;
import java.net.Socket;
import java.util.*;

public class Client {
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);

        while (true) {
            System.out.println("1. Stores near you");
            System.out.println("2. Filtering stores");
            System.out.println("3. Purchase products");
            System.out.println("4. Rate store");
            System.out.println("5. Exit");
            System.out.print("Choose an option: ");
            String option = sc.nextLine();

            if (option.equals("1")) {
                Socket requestSocket = null;
                ObjectOutputStream out = null;
                ObjectInputStream in = null;
                try {
                    requestSocket = new Socket("127.0.0.1", 4321);
                    out = new ObjectOutputStream(requestSocket.getOutputStream());
                    in = new ObjectInputStream(requestSocket.getInputStream());

                    out.writeObject("client");
                    out.flush();

                    System.out.print("Enter your latitude: ");
                    double lat = Double.parseDouble(sc.nextLine());

                    System.out.print("Enter your longitude: ");
                    double lon = Double.parseDouble(sc.nextLine());

                    // Create MapReduceRequest with default filters
                    MapReduceRequest request = new MapReduceRequest(
                            lat,
                            lon,
                            new ArrayList<>(), // No category filter
                            0,                // No minimum stars
                            "",               // No price filter
                            5.0               // 5km radius
                    );

                    out.writeObject(request);
                    out.flush();

                    System.out.println("Searching for stores nearby...");

                    ArrayList<Store> results = (ArrayList<Store>) in.readObject();

                    if (results.isEmpty()) {
                        System.out.println("No nearby stores found within 5 km.");
                    } else {
                        System.out.println("\nNearby Stores:");
                        for (Store store : results) {
                            System.out.println(store);
                            System.out.println("-----------");
                        }
                    }

                    out.writeObject("Done");
                    out.flush();

                } catch (Exception e) {
                    System.err.println("Error connecting to server: " + e.getMessage());
                    e.printStackTrace();
                }


            }else if (option.equals("2")) {
                Socket requestSocket = null;
                ObjectOutputStream out = null;
                ObjectInputStream in = null;
                try {
                    requestSocket = new Socket("127.0.0.1", 4321);
                    out = new ObjectOutputStream(requestSocket.getOutputStream());
                    in = new ObjectInputStream(requestSocket.getInputStream());


                    out.writeObject("filter");
                    out.flush();

                    System.out.print("Enter your latitude: ");
                    double latitude = Double.parseDouble(sc.nextLine());

                    System.out.print("Enter your longitude: ");
                    double longitude = Double.parseDouble(sc.nextLine());

                    System.out.print("Enter food categories (comma-separated, e.g., pizza,burger): ");
                    ArrayList<String> categories = new ArrayList<>(Arrays.asList(sc.nextLine().split("\\s*,\\s*")));

                    System.out.print("Enter minimum stars (1-5): ");
                    int minStars = Integer.parseInt(sc.nextLine());

                    System.out.print("Enter price category ($, $$, $$$): ");
                    String price = sc.nextLine();

                        MapReduceRequest request = new MapReduceRequest(
                                latitude,
                                longitude,
                                categories,
                                minStars,
                                price,
                                5.0 // radius in km
                        );

                        out.writeObject(request);
                        out.flush();

                        System.out.println("Searching with filters...");


                        ArrayList<Store> results = (ArrayList<Store>) in.readObject();

                        if (results.isEmpty()) {
                            System.out.println("No stores found matching your filters.");
                        } else {
                            System.out.println("\nFiltered Stores:");
                            for (Store store : results) {
                                System.out.println(store);
                                System.out.println("-----------");
                            }
                        }

                        out.writeObject("Done");
                        out.flush();

                    } catch (Exception e) {
                        System.err.println("Error connecting to server: " + e.getMessage());
                        e.printStackTrace();
                    }
            }else if (option.equals("3")) {
                Socket requestSocket = null;
                ObjectOutputStream out = null;
                ObjectInputStream in = null;
                try {
                    requestSocket = new Socket("127.0.0.1", 4321);
                    out = new ObjectOutputStream(requestSocket.getOutputStream());
                    in = new ObjectInputStream(requestSocket.getInputStream());

                    out.writeObject("fetchProducts");
                    out.flush();



                    // Store info
                    System.out.print("Enter store name you want to buy from: ");
                    String storeName = sc.nextLine();

                    out.writeObject(storeName);  // Send store name to fetch product list
                    out.flush();

                    ArrayList<Product> storeProducts = (ArrayList<Product>) in.readObject();

                    if (storeProducts.isEmpty()) {
                        System.out.println("No products available for this store.");
                        return;
                    }

                    System.out.println("\nAvailable products:");
                    for (Product p : storeProducts) {
                        System.out.println("- " + p.getName() + " (" + p.getCategory() + ") - " + p.getPrice() + "€ | Available: " + p.getQuantity());
                    }


                    out.writeObject("purchase");
                    out.flush();

                    // Products to purchase
                    ArrayList<Product> products = new ArrayList<>();
                    while (true) {
                        System.out.print("Enter product name (or type 'done' to finish): ");
                        String name = sc.nextLine();
                        if (name.equalsIgnoreCase("done")) break;

                        System.out.print("Enter quantity: ");
                        int quantity = Integer.parseInt(sc.nextLine());

                        // Create a product with only name and quantity
                        // Set category and price to placeholders — Worker will fill them in
                        products.add(new Product(name, "", quantity, 0.0));
                    }

                    // Buyer info
                    System.out.print("Enter your name: ");
                    String customerName = sc.nextLine();

                    System.out.print("Enter your email: ");
                    String email = sc.nextLine();

                    Purchase purchase = new Purchase(customerName, email, products);

                    out.writeObject(purchase);
                    out.flush();

                    // Response
                    String response = (String) in.readObject();
                    System.out.println("Server response: " + response);

                } catch (Exception e) {
                    System.err.println("Error during purchase: " + e.getMessage());
                    e.printStackTrace();
                }


            }else if (option.equals("4")) {
                Socket requestSocket = null;
                ObjectOutputStream out = null;
                ObjectInputStream in = null;
                try {
                    requestSocket = new Socket("127.0.0.1", 4321);
                    out = new ObjectOutputStream(requestSocket.getOutputStream());
                    in = new ObjectInputStream(requestSocket.getInputStream());

                    out.writeObject("rate");
                    out.flush();

                    System.out.print("Enter store name to rate: ");
                    String storeName = sc.nextLine();

                    System.out.print("Enter rating (1 to 5): ");
                    int rating = Integer.parseInt(sc.nextLine());

                    while (rating < 1 || rating > 5) {
                        System.out.print("Invalid rating. Please enter a number between 1 and 5: ");
                        rating = Integer.parseInt(sc.nextLine());
                    }

                    out.writeObject(storeName);
                    out.flush();
                    out.writeObject(rating);
                    out.flush();

                    String response = (String) in.readObject();
                    System.out.println("Server: " + response);

                } catch (Exception e) {
                    System.err.println("Error rating store: " + e.getMessage());
                    e.printStackTrace();
                }

            } else if (option.equals("5")) {
                System.out.println("Goodbye!");
                break;
            } else {
                System.out.println("Invalid option.");
            }
        }
    }


}
