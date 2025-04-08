import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.*;

public class Manager {
    public static void main(String[] args) throws ParseException, FileNotFoundException {
        Scanner sc = new Scanner(System.in);

        boolean flag = true;

        while (flag) {

            System.out.println("1.Add store");
            System.out.println("2.Add Product");
            System.out.println("3.Remove Product");
            System.out.println("4.Total sales by store type");
            System.out.println("5.Total sales by product category");
            System.out.println("6.Exit");
            System.out.println("Choose 1-2-3-4-5-6");
            String number = sc.nextLine();

            if (number.equals("1")) {
                ArrayList<Store> stores = new ArrayList<>();

                System.out.println("Give the json file of the store");
                String jsonPath = sc.nextLine();

                try (FileReader reader = new FileReader(jsonPath)) {

                    JSONParser parser = new JSONParser();

                    JSONArray jsonArray = (JSONArray) parser.parse(reader);


                    for (Object obj : jsonArray) {
                        JSONObject jsonObject = (JSONObject) obj;

                        String name = (String) jsonObject.get("StoreName");

                        double latitude = ((Number) jsonObject.get("Latitude")).doubleValue();

                        double longitude = ((Number) jsonObject.get("Longitude")).doubleValue();

                        String category = (String) jsonObject.get("FoodCategory");

                        int stars = ((Number) jsonObject.get("Stars")).intValue();

                        int reviews = ((Number) jsonObject.get("NoOfVotes")).intValue();

                        String storeLogoPath = (String) jsonObject.get("StoreLogo");

                        // Ανάγνωση των προϊόντων
                        ArrayList<Product> products = new ArrayList<>();
                        JSONArray productsArray = (JSONArray) jsonObject.get("Products");
                        for (Object prodObj : productsArray) {
                            JSONObject productJson = (JSONObject) prodObj;

                            String productName = (String) productJson.get("ProductName");
                            String productType = (String) productJson.get("ProductType");
                            // Διόρθωση: Χρησιμοποιούμε το productJson αντί για jsonObject
                            int amount = ((Number) productJson.get("Available Amount")).intValue();
                            double productPrice = ((Number) productJson.get("Price")).doubleValue();

                            products.add(new Product(productName, productType, amount, productPrice));
                        }

                        Store s = new Store(name, latitude, longitude, category, stars, reviews, storeLogoPath, products);

                        System.out.println(s);

                        stores.add(s);

                    }

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (org.json.simple.parser.ParseException e) {
                    throw new RuntimeException(e);
                }


                Socket requestSocket = null;
                ObjectOutputStream out = null;
                ObjectInputStream in = null;
                try {
                    requestSocket = new Socket("127.0.0.1", 4321);
                    out = new ObjectOutputStream(requestSocket.getOutputStream());
                    in = new ObjectInputStream(requestSocket.getInputStream());

                    out.writeObject("manager");
                    out.flush();

                    out.writeObject(stores);
                    out.flush();

                    String res = (String) in.readObject();
                    System.out.println(res);
                    System.out.print("\n");


                } catch (UnknownHostException unknownHost) {
                    System.err.println("You are trying to connect to an unknown host!");
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                } catch (ClassNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } finally {
                    try {
                        in.close();
                        out.close();
                        requestSocket.close();
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                }

            } else if (number.equals("2")) {

                Socket requestSocket = null;
                ObjectOutputStream out = null;
                ObjectInputStream in = null;

                try {
                    requestSocket = new Socket("127.0.0.1", 4321);
                    out = new ObjectOutputStream(requestSocket.getOutputStream());
                    in = new ObjectInputStream(requestSocket.getInputStream());

                    System.out.println("Enter store name to add a product:");
                    String storeName = sc.nextLine();

                    out.writeObject("findStore");
                    out.flush();

                    out.writeObject(storeName);
                    out.flush();

                    String s = (String) in.readObject();

                    Product pro = null;

                    //if the store doesn't exist the worker sends back null, otherwise sends the StoreName.
                    if (!(s == null)) {

                        System.out.println("Enter Product Name:");
                        String productName = sc.nextLine();

                        out.writeObject("findProduct");
                        out.flush();

                        out.writeObject(storeName);
                        out.flush();

                        out.writeObject(productName);
                        out.flush();


                        String p = (String) in.readObject();

                        //if the product doesn't exist the worker sends back null, otherwise sends the ProductName.
                        if (!(p == null)) {

                            System.out.println("Product already exists. How much would you like to add to the quantity?");
                            int additionalAmount = Integer.parseInt(sc.nextLine());

                            out.writeObject("AmountInc");
                            out.flush();

                            out.writeObject(storeName);
                            out.flush();

                            out.writeObject(productName);
                            out.flush();

                            out.writeInt(additionalAmount);
                            out.flush();


                        } else {

                            System.out.println("Enter Product Type:");
                            String productType = sc.nextLine();

                            System.out.println("Enter Available Amount:");
                            int amount = Integer.parseInt(sc.nextLine());

                            System.out.println("Enter Product Price:");
                            double productPrice = Double.parseDouble(sc.nextLine());

                            pro = new Product(productName, productType, amount, productPrice);

                            out.writeObject("NewProduct");
                            out.flush();

                            out.writeObject(storeName);
                            out.flush();

                            out.writeObject(pro);
                            out.flush();

                            System.out.println();

                        }

                        String res = (String) in.readObject();
                        System.out.println(res);
                        System.out.print("\n");

                    }else System.out.println("Store not found.");
                } catch (UnknownHostException unknownHost) {
                System.err.println("You are trying to connect to an unknown host!");
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                } finally {
                    try {
                        in.close();
                        out.close();
                        requestSocket.close();
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                }


            } else if (number.equals("3")) {

                Socket requestSocket = null;
                ObjectOutputStream out = null;
                ObjectInputStream in = null;

                try {

                    requestSocket = new Socket("127.0.0.1", 4321);
                    out = new ObjectOutputStream(requestSocket.getOutputStream());
                    in = new ObjectInputStream(requestSocket.getInputStream());

                    System.out.println("Enter store name to remove a product:");
                    String storeName = sc.nextLine();

                    out.writeObject("findStore");
                    out.flush();

                    out.writeObject(storeName);
                    out.flush();

                    String s = (String) in.readObject();

                    Product pro = null;

                    //if the store doesn't exist the worker sends back null, otherwise sends the StoreName.
                    if (!(s == null)) {

                        System.out.println("Enter Product Name:");
                        String productName = sc.nextLine();

                        out.writeObject("findProduct");
                        out.flush();

                        out.writeObject(storeName);
                        out.flush();

                        out.writeObject(productName);
                        out.flush();


                        String p = (String) in.readObject();

                        //if the product doesn't exist the worker sends back null, otherwise sends the ProductName.
                        if (!(p == null)) {

                            System.out.println("1. Remove the product");
                            System.out.println("2. Decrease the quantity of the product");
                            System.out.println("Choose 1-2");
                            String num = sc.nextLine();

                            if(num.equals("1")){
                                out.writeObject("remove");
                                out.flush();

                                out.writeObject(storeName);
                                out.flush();

                                out.writeObject(productName);
                                out.flush();

                            } else if (num.equals("2")) {

                                System.out.println("How much would you like to decrease the quantity?");
                                int amount = Integer.parseInt(sc.nextLine());

                                out.writeObject("AmountDec");
                                out.flush();

                                out.writeObject(storeName);
                                out.flush();

                                out.writeObject(productName);
                                out.flush();

                                out.writeInt(amount);
                                out.flush();

                            }


                        } else {
                            System.out.println("The product doesn't exist");
                        }

                        String res = (String) in.readObject();
                        System.out.println(res);
                        System.out.print("\n");

                    }else System.out.println("Store not found.");
                } catch (UnknownHostException unknownHost) {
                    System.err.println("You are trying to connect to an unknown host!");
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                } finally {
                    try {
                        in.close();
                        out.close();
                        requestSocket.close();
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                }

            } else if (number.equals("4")) {
                System.out.println("Enter the store type (e.g., pizzeria, burger):");
                String storeType = sc.nextLine();

                Socket requestSocket = null;
                ObjectOutputStream out = null;
                ObjectInputStream in = null;
                try{
                    requestSocket = new Socket("127.0.0.1", 4321);
                    out = new ObjectOutputStream(requestSocket.getOutputStream());
                    in = new ObjectInputStream(requestSocket.getInputStream());


                    out.writeObject("storeType");
                    out.flush();

                    out.writeObject(storeType);
                    out.flush();


                    Map<String, Integer> result = (Map<String, Integer>) in.readObject();

                    int total = 0;
                    System.out.println("Sales by Store for type: " + storeType);
                    for (Map.Entry<String, Integer> entry : result.entrySet()) {
                        System.out.println("• " + entry.getKey() + ": " + entry.getValue());
                        total += entry.getValue();
                    }
                    System.out.println("Total Sales: " + total + "\n");

                } catch (UnknownHostException unknownHost) {
                    System.err.println("You are trying to connect to an unknown host!");
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                } finally {
                    try {
                        in.close();
                        out.close();
                        requestSocket.close();
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                }


            } else if (number.equals("5")) {
                System.out.println("Enter the product category (e.g., pizza, salad, burger):");
                String productCategory = sc.nextLine();

                Socket requestSocket = null;
                ObjectOutputStream out = null;
                ObjectInputStream in = null;
                try {
                    requestSocket = new Socket("127.0.0.1", 4321);
                    out = new ObjectOutputStream(requestSocket.getOutputStream());
                    in = new ObjectInputStream(requestSocket.getInputStream());

                    out.writeObject("productCategory");
                    out.flush();

                    out.writeObject(productCategory);
                    out.flush();

                    Map<String, Integer> result = (Map<String, Integer>) in.readObject();

                    int total = 0;
                    System.out.println("Sales by Store for product category: " + productCategory);
                    for (Map.Entry<String, Integer> entry : result.entrySet()) {
                        System.out.println("• " + entry.getKey() + ": " + entry.getValue());
                        total += entry.getValue();
                    }
                    System.out.println("Total Sales: " + total + "\n");



                } catch (UnknownHostException unknownHost) {
                    System.err.println("You are trying to connect to an unknown host!");
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                } finally {
                    try {
                        in.close();
                        out.close();
                        requestSocket.close();
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                }


            } else if (number.equals("6")) {
                System.out.println("Exit");
                flag = false;
            } else {
                System.out.println("Wrong number. Try again");
            }
        }

    }
}