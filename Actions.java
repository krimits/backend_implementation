import java.io.*;
import java.net.*;
import java.text.ParseException;
import java.util.*;

public class Actions extends Thread {
    ObjectInputStream in;
    ObjectOutputStream out;
    String[][] workers;
    HashMap<Integer, ObjectOutputStream> connectionsOut;
    int counterID;

    public Actions(Socket connection, String[][] workers, HashMap<Integer, ObjectOutputStream> connectionsOut, int counterID) {
        try {
            out = new ObjectOutputStream(connection.getOutputStream());
            in = new ObjectInputStream(connection.getInputStream());
            this.workers = workers;
            this.connectionsOut = connectionsOut;
            this.counterID = counterID;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            String role = (String) in.readObject();

            if (role.equals("manager")) {
                // Receive the store list
                ArrayList<Store> stores = (ArrayList<Store>) in.readObject();
                int successCount = 0;

                for (Store store : stores) {
                    Socket workerSocket = null;
                    ObjectOutputStream outWorker = null;
                    ObjectInputStream inWorker = null;

                    try {
                        // Hash-based assignment to worker
                        int workerId = Math.abs(store.getStoreName().hashCode()) % workers.length;
                        String workerIP = workers[workerId][0];
                        int workerPort = Integer.parseInt(workers[workerId][1]);

                        workerSocket = new Socket(workerIP, workerPort);
                        outWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                        inWorker = new ObjectInputStream(workerSocket.getInputStream());

                        outWorker.writeObject("manager");
                        outWorker.flush();

                        outWorker.writeObject(store);
                        outWorker.flush();

                        String response = (String) inWorker.readObject();
                        if ("Store(s) added successfully".equals(response)) {
                            successCount++;
                        }

                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (inWorker != null) inWorker.close();
                            if (outWorker != null) outWorker.close();
                            if (workerSocket != null) workerSocket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }

                if (successCount == stores.size()) {
                    out.writeObject("Store(s) added successfully");
                } else {
                    out.writeObject("Some stores failed to add");
                }
                out.flush();


            }else if (role.equals("findStore")) {
                String storeName = (String) in.readObject();       // Get store name

                Socket workerSocket = null;
                ObjectOutputStream outWorker = null;
                ObjectInputStream inWorker = null;

                try {
                    int workerId = Math.abs(storeName.hashCode()) % workers.length;
                    String workerIP = workers[workerId][0];
                    int workerPort = Integer.parseInt(workers[workerId][1]);

                    workerSocket = new Socket(workerIP, workerPort);
                    outWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                    inWorker = new ObjectInputStream(workerSocket.getInputStream());

                    // Send operation and data
                    outWorker.writeObject("findStore");
                    outWorker.flush();

                    outWorker.writeObject(storeName);
                    outWorker.flush();


                    // Get response from worker and pass to manager
                    String response = (String) inWorker.readObject();
                    out.writeObject(response);
                    out.flush();

                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (inWorker != null) inWorker.close();
                        if (outWorker != null) outWorker.close();
                        if (workerSocket != null) workerSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }else if (role.equals("findProduct")) {
                    String storeName = (String) in.readObject();       // Get store name
                    String ProductName = (String) in.readObject();

                    Socket workerSocket = null;
                    ObjectOutputStream outWorker = null;
                    ObjectInputStream inWorker = null;

                    try {
                        int workerId = Math.abs(storeName.hashCode()) % workers.length;
                        String workerIP = workers[workerId][0];
                        int workerPort = Integer.parseInt(workers[workerId][1]);

                        workerSocket = new Socket(workerIP, workerPort);
                        outWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                        inWorker = new ObjectInputStream(workerSocket.getInputStream());

                        // Send operation and data
                        outWorker.writeObject("findProduct");
                        outWorker.flush();

                        outWorker.writeObject(storeName);
                        outWorker.flush();

                        outWorker.writeObject(ProductName);
                        outWorker.flush();


                        // Get response from worker and pass to manager
                        String response = (String) inWorker.readObject();
                        out.writeObject(response);
                        out.flush();

                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (inWorker != null) inWorker.close();
                            if (outWorker != null) outWorker.close();
                            if (workerSocket != null) workerSocket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

            }else if (role.equals("AmountInc")) {
                String storeName = (String) in.readObject();       // Get store name
                String ProductName = (String) in.readObject();
                int amount = (int) in.readInt();

                Socket workerSocket = null;
                ObjectOutputStream outWorker = null;
                ObjectInputStream inWorker = null;

                try {
                    int workerId = Math.abs(storeName.hashCode()) % workers.length;
                    String workerIP = workers[workerId][0];
                    int workerPort = Integer.parseInt(workers[workerId][1]);

                    workerSocket = new Socket(workerIP, workerPort);
                    outWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                    inWorker = new ObjectInputStream(workerSocket.getInputStream());

                    // Send operation and data
                    outWorker.writeObject("AmountInc");
                    outWorker.flush();

                    outWorker.writeObject(storeName);
                    outWorker.flush();

                    outWorker.writeObject(ProductName);
                    outWorker.flush();

                    out.writeInt(amount);
                    out.flush();


                    // Get response from worker and pass to manager
                    String response = (String) inWorker.readObject();
                    out.writeObject(response);
                    out.flush();

                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (inWorker != null) inWorker.close();
                        if (outWorker != null) outWorker.close();
                        if (workerSocket != null) workerSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }else if (role.equals("NewProduct")) {
                String storeName = (String) in.readObject();       // Get store name
                Product pro = (Product) in.readObject();

                Socket workerSocket = null;
                ObjectOutputStream outWorker = null;
                ObjectInputStream inWorker = null;

                try {
                    int workerId = Math.abs(storeName.hashCode()) % workers.length;
                    String workerIP = workers[workerId][0];
                    int workerPort = Integer.parseInt(workers[workerId][1]);

                    workerSocket = new Socket(workerIP, workerPort);
                    outWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                    inWorker = new ObjectInputStream(workerSocket.getInputStream());

                    // Send operation and data
                    outWorker.writeObject("NewProduct");
                    outWorker.flush();

                    outWorker.writeObject(storeName);
                    outWorker.flush();

                    outWorker.writeObject(pro);
                    outWorker.flush();

                    // Get response from worker and pass to manager
                    String response = (String) inWorker.readObject();
                    out.writeObject(response);
                    out.flush();

                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (inWorker != null) inWorker.close();
                        if (outWorker != null) outWorker.close();
                        if (workerSocket != null) workerSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }else if (role.equals("remove")) {
                String storeName = (String) in.readObject();         // Get the store name
                String productName = (String) in.readObject();         // Get the product name

                Socket workerSocket = null;
                ObjectOutputStream outWorker = null;
                ObjectInputStream inWorker = null;

                try {
                    // Determine the worker responsible for this store
                    int workerId = Math.abs(storeName.hashCode()) % workers.length;
                    String workerIP = workers[workerId][0];
                    int workerPort = Integer.parseInt(workers[workerId][1]);

                    // Connect to the appropriate worker
                    workerSocket = new Socket(workerIP, workerPort);
                    outWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                    inWorker = new ObjectInputStream(workerSocket.getInputStream());

                    // Send command and data to worker
                    outWorker.writeObject("remove");
                    outWorker.flush();

                    outWorker.writeObject(storeName);
                    outWorker.flush();

                    outWorker.writeObject(productName);
                    outWorker.flush();

                    // Wait for confirmation and forward it to manager
                    String response = (String) inWorker.readObject();
                    out.writeObject(response);
                    out.flush();

                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (inWorker != null) inWorker.close();
                        if (outWorker != null) outWorker.close();
                        if (workerSocket != null) workerSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }else if (role.equals("AmountDec")) {
                String storeName = (String) in.readObject();       // Get store name
                String ProductName = (String) in.readObject();
                int amount = (int) in.readInt();

                Socket workerSocket = null;
                ObjectOutputStream outWorker = null;
                ObjectInputStream inWorker = null;

                try {
                    int workerId = Math.abs(storeName.hashCode()) % workers.length;
                    String workerIP = workers[workerId][0];
                    int workerPort = Integer.parseInt(workers[workerId][1]);

                    workerSocket = new Socket(workerIP, workerPort);
                    outWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                    inWorker = new ObjectInputStream(workerSocket.getInputStream());

                    // Send operation and data
                    outWorker.writeObject("AmountDec");
                    outWorker.flush();

                    outWorker.writeObject(storeName);
                    outWorker.flush();

                    outWorker.writeObject(ProductName);
                    outWorker.flush();

                    out.writeInt(amount);
                    out.flush();


                    // Get response from worker and pass to manager
                    String response = (String) inWorker.readObject();
                    out.writeObject(response);
                    out.flush();

                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (inWorker != null) inWorker.close();
                        if (outWorker != null) outWorker.close();
                        if (workerSocket != null) workerSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }


            }else if (role.equals("storeType")) {
                String storeType = (String) in.readObject();  // e.g., "pizzeria"

                try {
                    // Χρησιμοποιούμε την κλάση MapReduceImplementation για την εφαρμογή του μοντέλου MapReduce
                    MapReduceImplementation mapReduce = new MapReduceImplementation(workers);
                    Map<String, Integer> finalResult = mapReduce.executeMapReduceStoreType(storeType);
                    
                    // Στέλνουμε τα αποτελέσματα πίσω στον manager
                    out.writeObject(finalResult);
                    out.flush();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                    out.writeObject(new HashMap<String, Integer>());
                    out.flush();
                }


            }else if (role.equals("productCategory")) {
                String productCategory = (String) in.readObject(); // e.g., "pizza"

                try {
                    // Χρησιμοποιούμε την κλάση MapReduceImplementation για την εφαρμογή του μοντέλου MapReduce
                    MapReduceImplementation mapReduce = new MapReduceImplementation(workers);
                    Map<String, Integer> finalResult = mapReduce.executeMapReduceProductCategory(productCategory);
                    
                    // Στέλνουμε τα αποτελέσματα πίσω στον manager
                    out.writeObject(finalResult);
                    out.flush();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                    out.writeObject(new HashMap<String, Integer>());
                    out.flush();
                }


            }else if (role.equals("client")) {

                MapReduceRequest request = (MapReduceRequest) in.readObject();

                ArrayList <Store> finalResult = new ArrayList<> () ;
                for (int i = 0; i < workers.length; i++) {
                    Socket workerSocket = null;
                    ObjectOutputStream outWorker = null;
                    ObjectInputStream inWorker = null;

                    try {
                        String workerIP = workers[i][0];
                        int workerPort = Integer.parseInt(workers[i][1]);

                        workerSocket = new Socket(workerIP, workerPort);
                        outWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                        inWorker = new ObjectInputStream(workerSocket.getInputStream());

                        // Send operation type and category to filter
                        outWorker.writeObject("client");
                        outWorker.flush();

                        outWorker.writeObject(request);
                        outWorker.flush();

                        finalResult = (ArrayList<Store>) inWorker.readObject();


                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (inWorker != null) inWorker.close();
                            if (outWorker != null) outWorker.close();
                            if (workerSocket != null) workerSocket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    // Send final result to the Manager
                    out.writeObject(finalResult);
                    out.flush();

                }



            }else if (role.equals("filter")) {
                MapReduceRequest request = (MapReduceRequest) in.readObject();

                try {
                    // Χρησιμοποιούμε την κλάση MapReduceImplementation για την εφαρμογή του μοντέλου MapReduce
                    MapReduceImplementation mapReduce = new MapReduceImplementation(workers);
                    ArrayList<Store> results = mapReduce.executeMapReduceSearch(request);
                    
                    // Στέλνουμε τα αποτελέσματα πίσω στον client
                    out.writeObject(results);
                    out.flush();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                    out.writeObject(new ArrayList<Store>());
                    out.flush();
                }


            }else if (role.equals("fetchProducts")) {

                String store = (String) in.readObject();

                ArrayList<Product> results = new ArrayList<>();

                for (int i = 0; i < workers.length; i++) {
                    Socket workerSocket = null;
                    ObjectOutputStream outWorker = null;
                    ObjectInputStream inWorker = null;

                    try {
                        String workerIP = workers[i][0];
                        int workerPort = Integer.parseInt(workers[i][1]);

                        workerSocket = new Socket(workerIP, workerPort);
                        outWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                        inWorker = new ObjectInputStream(workerSocket.getInputStream());

                        // Send operation type and category to filter
                        outWorker.writeObject("fetchProducts");
                        outWorker.flush();

                        outWorker.writeObject(store);
                        outWorker.flush();

                        results = (ArrayList<Product>) inWorker.readObject();


                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (inWorker != null) inWorker.close();
                            if (outWorker != null) outWorker.close();
                            if (workerSocket != null) workerSocket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }

                // Send final result to the Manager
                out.writeObject(results);
                out.flush();


            }else if (role.equals("purchase")) {

                Purchase pur = (Purchase) in.readObject();
                String results = null;

                for (int i = 0; i < workers.length; i++) {
                    Socket workerSocket = null;
                    ObjectOutputStream outWorker = null;
                    ObjectInputStream inWorker = null;

                    try {
                        String workerIP = workers[i][0];
                        int workerPort = Integer.parseInt(workers[i][1]);

                        workerSocket = new Socket(workerIP, workerPort);
                        outWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                        inWorker = new ObjectInputStream(workerSocket.getInputStream());

                        // Send operation type and category to filter
                        outWorker.writeObject("purchase");
                        outWorker.flush();

                        outWorker.writeObject(pur);
                        outWorker.flush();

                       results = (String) inWorker.readObject();


                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (inWorker != null) inWorker.close();
                            if (outWorker != null) outWorker.close();
                            if (workerSocket != null) workerSocket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }

                // Send final result to the Manager
                out.writeObject(results);
                out.flush();


            }else if (role.equals("rate")) {

                String storeName = (String) in.readObject();
                int rating = (int) in.readObject();

                String results = null;

                // Διόρθωση: Χρησιμοποιούμε το hash του ονόματος του καταστήματος για να βρούμε τον σωστό worker
                int workerId = Math.abs(storeName.hashCode()) % workers.length;
                String workerIP = workers[workerId][0];
                int workerPort = Integer.parseInt(workers[workerId][1]);

                Socket workerSocket = null;
                ObjectOutputStream outWorker = null;
                ObjectInputStream inWorker = null;

                try {
                    workerSocket = new Socket(workerIP, workerPort);
                    outWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                    inWorker = new ObjectInputStream(workerSocket.getInputStream());

                    // Διόρθωση: Στέλνουμε τη σωστή εντολή "rate" αντί για "purchase"
                    outWorker.writeObject("rate");
                    outWorker.flush();

                    outWorker.writeObject(storeName);
                    outWorker.flush();

                    outWorker.writeObject(rating);
                    outWorker.flush();

                    results = (String) inWorker.readObject();

                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (inWorker != null) inWorker.close();
                        if (outWorker != null) outWorker.close();
                        if (workerSocket != null) workerSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                // Send final result to the Manager
                out.writeObject(results);
                out.flush();
            }








//            } else if (role.equals("reducer")) {
//                // Συγχώνευση αποτελεσμάτων από Workers (Reduce)
//                int id = in.readInt();
//                Map<String, List<Store>> result = (Map<String, List<Store>>) in.readObject();
//
//                connectionsOut.get(id).writeObject(result);
//                connectionsOut.get(id).flush();
//
//            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
                out.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
       }
    }
}
