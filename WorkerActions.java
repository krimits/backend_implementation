import java.awt.print.Book;
import java.io.*;
import java.net.*;
import java.util.*;

public class WorkerActions extends Thread {
    ObjectInputStream in;
    ObjectOutputStream out;
    private final ArrayList<Store> stores;
    private final Object lock;
    private final Socket connection;

    public WorkerActions(Socket connection, ArrayList<Store> stores, Object lock) {
        this.connection = connection;
        this.stores = stores;
        this.lock = lock;
        try {
            out = new ObjectOutputStream(connection.getOutputStream());
            in = new ObjectInputStream(connection.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            String role = (String) in.readObject();

            if (role.equals("manager")) {
                Store s = (Store) in.readObject();

                stores.add(s);

                out.writeObject("Room(s) added successfully");
                out.flush();


            }else if (role.equals("findStore")) {
                String storeName = (String) in.readObject();

                boolean storeFound = false;

                synchronized (lock) {
                    for (Store store : stores) {
                        if (store.getStoreName().equalsIgnoreCase(storeName)) {
                            storeFound = true;

                            break; // exit after store is found and processed
                        }
                    }
                }

                if (!storeFound) {
                    storeName = null;
                }
                out.writeObject(storeName);
                out.flush();

            }else if (role.equals("findProduct")) {
                String storeName = (String) in.readObject();
                String ProductName = (String) in.readObject();

                boolean productFound = false;

                synchronized (lock) {
                    for (Store store : stores) {
                        if (store.getStoreName().equalsIgnoreCase(storeName)) {
                            for (Product pro : store.getProducts()) {
                                if (pro.getName().equalsIgnoreCase(ProductName)) {
                                    productFound = true;

                                    break; // exit after product is found
                                }
                            }
                        }
                    }
                }

                if (!productFound) {
                    ProductName = null;
                }
                out.writeObject(ProductName);
                out.flush();

            }else if (role.equals("AmountInc")) {
                String storeName = (String) in.readObject();
                String ProductName = (String) in.readObject();
                int amount = (int) in.readInt();

                synchronized (lock) {
                    for (Store store : stores) {
                        if (store.getStoreName().equalsIgnoreCase(storeName)) {
                            for (Product pro : store.getProducts()) {
                                if (pro.getName().equalsIgnoreCase(ProductName)) {

                                    pro.setQuantity(amount + pro.getQuantity());

                                    break; // exit after quantity is changed
                                }
                            }
                        }
                    }
                }



                out.writeObject("Amount changed successfully");
                out.flush();

            }else if (role.equals("NewProduct")) {
                String storeName = (String) in.readObject();
                Product pro = (Product) in.readObject();


                synchronized (lock) {
                    for (Store store : stores) {
                        if (store.getStoreName().equalsIgnoreCase(storeName)) {

                            store.getProducts().add(pro);
                            break; // exit after product is added

                        }
                    }
                }

                out.writeObject("Product added successfully");
                out.flush();

            }else if (role.equals("remove")) {
                String storeName = (String) in.readObject();
                String pro = (String) in.readObject();

                boolean storeFound = false;
                synchronized (lock) {
                    for (Store store : stores) {
                        if (store.getStoreName().equalsIgnoreCase(storeName)) {
                            storeFound = true;
                            for (Product prod : store.getProducts()) {
                                if (prod.getName().equalsIgnoreCase(pro)) {

                                    prod.setQuantity(-1);
                                    prod.setStatus("hidden");

                                    break; // exit after quantity is changed
                                }
                            }

                            break;
                        }
                    }
                }

                if (storeFound) {
                    out.writeObject("Product removed or updated successfully.");
                } else {
                    out.writeObject("Store not found.");
                }
                out.flush();

            }else if (role.equals("AmountDec")) {
                String storeName = (String) in.readObject();
                String ProductName = (String) in.readObject();
                int amount = (int) in.readInt();

                boolean productFound = false;
                boolean amountValid = false;

                synchronized (lock) {
                    for (Store store : stores) {
                        if (store.getStoreName().equalsIgnoreCase(storeName)) {
                            for (Product pro : store.getProducts()) {
                                if (pro.getName().equalsIgnoreCase(ProductName)) {
                                    productFound = true;
                                    if ((pro.getQuantity() - amount) >= 0) {
                                        pro.setQuantity(pro.getQuantity() - amount);
                                        amountValid = true;
                                    }
                                    break;
                                }
                            }
                            break;
                        }
                    }
                }
                
                if (!productFound) {
                    out.writeObject("Product not found");
                } else if (!amountValid) {
                    out.writeObject("Amount is greater than the quantity");
                } else {
                    out.writeObject("Amount changed successfully");
                }
                out.flush();


            }else if (role.equals("storeType")) {
                String requestedType = (String) in.readObject(); // e.g., "pizzeria"

                Map<String, Integer> result = new HashMap<>();

                synchronized (lock) {
                    for (Store store : stores) {
                        if (store.getCategory().equalsIgnoreCase(requestedType)) {
                            int totalSold = 0;

                            for (Purchase purchase : store.getPurchases()) {
                                for (Product p : purchase.getPurchasedProducts()) {
                                    totalSold += p.getQuantity();  // Sum all quantities
                                }
                            }

                            result.put(store.getStoreName(), totalSold);
                        }
                    }
                }

                out.writeObject(result);
                out.flush();


            }else if (role.equals("productCategory")) {
                String requestedCategory = (String) in.readObject(); // e.g., "pizza"

                Map<String, Integer> result = new HashMap<>();

                synchronized (lock) {
                    for (Store store : stores) {
                        int totalCategorySales = 0;

                        for (Purchase purchase : store.getPurchases()) {
                            for (Product product : purchase.getPurchasedProducts()) {
                                if (product.getCategory().equalsIgnoreCase(requestedCategory)) {
                                    totalCategorySales += product.getQuantity();
                                }
                            }
                        }

                        if (totalCategorySales > 0) {
                            result.put(store.getStoreName(), totalCategorySales);
                        }
                    }
                }

                out.writeObject(result);
                out.flush();



            }else if (role.equals("client")) {
                MapReduceRequest request = (MapReduceRequest) in.readObject();

                double userLat = request.getClientLatitude();
                double userLon = request.getClientLongitude();
                double maxDistance = request.getRadius();

                ArrayList<Store> result = new ArrayList<>();

                synchronized (lock) {
                    for (Store store : stores) {
                        double storeLat = store.getLatitude();
                        double storeLon = store.getLongitude();

                        double distance = Math.sqrt(Math.pow(userLat - storeLat, 2) + Math.pow(userLon - storeLon, 2));

                        if (distance <= maxDistance) {
                            result.add(store);
                        }
                    }
                }

                out.writeObject(result);
                out.flush();

                // Optional: read confirmation from client
                String confirm = (String) in.readObject();
                if (!"Done".equalsIgnoreCase(confirm)) {
                    System.out.println("Client did not confirm receipt of store list.");
                }


            }else if (role.equals("filter")) {
                MapReduceRequest request = (MapReduceRequest) in.readObject();

                double userLat = request.getClientLatitude();
                double userLon = request.getClientLongitude();
                double radius = request.getRadius();

                ArrayList<String> categories = (ArrayList<String>) request.getFoodCategories();  // optional
                int minStars = request.getMinStars();                  // optional
                String price = request.getPriceCategory();             // optional

                ArrayList<Store> result = new ArrayList<>();

                synchronized (lock) {
                    for (Store store : stores) {
                        double distance = Math.sqrt(Math.pow(userLat - store.getLatitude(), 2) + Math.pow(userLon - store.getLongitude(), 2));

                        boolean matchesDistance = distance <= radius;
                        boolean matchesCategory = categories.isEmpty() || categories.contains(store.getCategory());
                        boolean matchesStars = minStars == 0 || store.getStars() >= minStars;
                        boolean matchesPrice = price.isEmpty() || store.calculatePriceCategory().equalsIgnoreCase(price);

                        if (matchesDistance && matchesCategory && matchesStars && matchesPrice) {
                            result.add(store);
                        }
                    }
                }

                out.writeObject(result);
                out.flush();

                // Optional confirmation
                String confirm = (String) in.readObject();
                if (!"Done".equalsIgnoreCase(confirm)) {
                    System.out.println("Client didn't confirm receipt of filtered results.");
                }


            }else if (role.equals("fetchProducts")) {
                String storeName = (String) in.readObject();

                ArrayList<Product> available = new ArrayList<>();

                synchronized (lock) {
                    for (Store store : stores) {
                        if (store.getStoreName().equalsIgnoreCase(storeName)) {
                            for (Product product : store.getProducts()) {
                                if (product.getStatus().equalsIgnoreCase("visible")) {
                                    available.add(product);
                                }
                            }
                            break;
                        }
                    }
                }

                out.writeObject(available);
                out.flush();



            }else if (role.equals("purchase")) {
                Purchase purchase = (Purchase) in.readObject();
                ArrayList<Product> requestedProducts = purchase.getPurchasedProducts();

                boolean storeFound = false;
                String storeName = "";

                synchronized (lock) {
                    for (Store store : stores) {
                        Map<String, Product> storeProductMap = new HashMap<>();
                        for (Product p : store.getProducts()) {
                            // Μόνο τα ορατά προϊόντα μπορούν να αγοραστούν
                            if (p.getStatus().equalsIgnoreCase("visible")) {
                                storeProductMap.put(p.getName().toLowerCase(), p);
                            }
                        }

                        boolean allMatch = true;
                        for (Product req : requestedProducts) {
                            Product available = storeProductMap.get(req.getName().toLowerCase());
                            if (available == null || available.getQuantity() < req.getQuantity()) {
                                allMatch = false;
                                break;
                            }
                        }

                        if (allMatch) {
                            for (Product req : requestedProducts) {
                                Product prod = storeProductMap.get(req.getName().toLowerCase());
                                // Ενημερώνουμε το απόθεμα
                                prod.setQuantity(prod.getQuantity() - req.getQuantity());

                                // Συμπληρώνουμε τα στοιχεία του προϊόντος στην αγορά
                                req.setCategory(prod.getCategory());
                                req.setPrice(prod.getPrice());
                            }

                            // Προσθέτουμε την αγορά στο ιστορικό του καταστήματος
                            store.getPurchases().add(purchase);
                            storeName = store.getStoreName();
                            storeFound = true;
                            break;
                        }
                    }
                }

                if (!storeFound) {
                    out.writeObject("Purchase failed: No store has all requested items in sufficient quantity.");
                } else {
                    out.writeObject("Purchase completed successfully at store: " + storeName);
                }

                out.flush();



            }else if (role.equals("rate")) {
                String storeName = (String) in.readObject();
                int rating = (int) in.readObject();

                boolean storeFound = false;

                synchronized (lock) {
                    for (Store store : stores) {
                        if (store.getStoreName().equalsIgnoreCase(storeName)) {
                            int oldStars = store.getStars();          // current average rating
                            int oldReviews = store.getNoOfReviews();  // total reviews so far

                            int newReviews = oldReviews + 1;
                            int newAvg = (int) Math.round((oldStars * oldReviews + rating) / (double) newReviews);

                            // Update store fields
                            store.setStars(newAvg);          // use setters if fields are private
                            store.setNoOfReviews(newReviews);

                            storeFound = true;
                            break;
                        }
                    }
                }

                if (storeFound) {
                    out.writeObject("Rating submitted successfully.");
                } else {
                    out.writeObject("Store not found.");
                }
                out.flush();
            }



























        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) in.close();
                if (out != null) out.close();
                if (connection != null && !connection.isClosed()) connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}