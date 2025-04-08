import java.io.*;
import java.net.*;
import java.util.*;

public class MapReduceImplementation {
    private final String[][] workers;
    private final int numWorkers;
    
    public MapReduceImplementation(String[][] workers) {
        this.workers = workers;
        this.numWorkers = workers.length;
    }
    
    /**
     * Εκτελεί την φάση Map του MapReduce για αναζήτηση καταστημάτων με φίλτρα
     * @param request Το αίτημα με τα φίλτρα αναζήτησης
     * @return Λίστα με τα καταστήματα που ταιριάζουν στα φίλτρα
     */
    public ArrayList<Store> executeMapReduceSearch(MapReduceRequest request) throws IOException, ClassNotFoundException {
        // Φάση Map: Στέλνουμε το αίτημα σε όλους τους workers
        ArrayList<ArrayList<Store>> partialResults = new ArrayList<>();
        
        for (int i = 0; i < numWorkers; i++) {
            Socket workerSocket = null;
            ObjectOutputStream outWorker = null;
            ObjectInputStream inWorker = null;
            
            try {
                String workerIP = workers[i][0];
                int workerPort = Integer.parseInt(workers[i][1]);
                
                workerSocket = new Socket(workerIP, workerPort);
                outWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                inWorker = new ObjectInputStream(workerSocket.getInputStream());
                
                // Στέλνουμε την εντολή και το αίτημα
                outWorker.writeObject("filter");
                outWorker.flush();
                
                outWorker.writeObject(request);
                outWorker.flush();
                
                // Λαμβάνουμε τα μερικά αποτελέσματα
                ArrayList<Store> workerResult = (ArrayList<Store>) inWorker.readObject();
                partialResults.add(workerResult);
                
            } finally {
                if (inWorker != null) inWorker.close();
                if (outWorker != null) outWorker.close();
                if (workerSocket != null) workerSocket.close();
            }
        }
        
        // Φάση Reduce: Συνδυάζουμε τα αποτελέσματα
        return reduceSearchResults(partialResults);
    }
    
    /**
     * Συνδυάζει τα μερικά αποτελέσματα από τους workers
     * @param partialResults Τα μερικά αποτελέσματα από κάθε worker
     * @return Η τελική λίστα καταστημάτων
     */
    private ArrayList<Store> reduceSearchResults(ArrayList<ArrayList<Store>> partialResults) {
        ArrayList<Store> finalResults = new ArrayList<>();
        
        // Συνδυάζουμε όλα τα μερικά αποτελέσματα
        for (ArrayList<Store> workerResult : partialResults) {
            finalResults.addAll(workerResult);
        }
        
        // Αφαιρούμε τυχόν διπλότυπα (με βάση το όνομα του καταστήματος)
        Map<String, Store> uniqueStores = new HashMap<>();
        for (Store store : finalResults) {
            uniqueStores.put(store.getStoreName(), store);
        }
        
        return new ArrayList<>(uniqueStores.values());
    }
    
    /**
     * Εκτελεί την φάση Map του MapReduce για συγκέντρωση πωλήσεων ανά τύπο καταστήματος
     * @param storeType Ο τύπος καταστήματος για τον οποίο θέλουμε τις πωλήσεις
     * @return Map με τις πωλήσεις ανά κατάστημα και το συνολικό άθροισμα
     */
    public Map<String, Integer> executeMapReduceStoreType(String storeType) throws IOException, ClassNotFoundException {
        // Φάση Map: Στέλνουμε το αίτημα σε όλους τους workers
        ArrayList<Map<String, Integer>> partialResults = new ArrayList<>();
        
        for (int i = 0; i < numWorkers; i++) {
            Socket workerSocket = null;
            ObjectOutputStream outWorker = null;
            ObjectInputStream inWorker = null;
            
            try {
                String workerIP = workers[i][0];
                int workerPort = Integer.parseInt(workers[i][1]);
                
                workerSocket = new Socket(workerIP, workerPort);
                outWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                inWorker = new ObjectInputStream(workerSocket.getInputStream());
                
                // Στέλνουμε την εντολή και τον τύπο καταστήματος
                outWorker.writeObject("storeType");
                outWorker.flush();
                
                outWorker.writeObject(storeType);
                outWorker.flush();
                
                // Λαμβάνουμε τα μερικά αποτελέσματα
                Map<String, Integer> workerResult = (Map<String, Integer>) inWorker.readObject();
                partialResults.add(workerResult);
                
            } finally {
                if (inWorker != null) inWorker.close();
                if (outWorker != null) outWorker.close();
                if (workerSocket != null) workerSocket.close();
            }
        }
        
        // Φάση Reduce: Συνδυάζουμε τα αποτελέσματα
        return reduceAggregationResults(partialResults);
    }
    
    /**
     * Εκτελεί την φάση Map του MapReduce για συγκέντρωση πωλήσεων ανά κατηγορία προϊόντος
     * @param productCategory Η κατηγορία προϊόντος για την οποία θέλουμε τις πωλήσεις
     * @return Map με τις πωλήσεις ανά κατάστημα και το συνολικό άθροισμα
     */
    public Map<String, Integer> executeMapReduceProductCategory(String productCategory) throws IOException, ClassNotFoundException {
        // Φάση Map: Στέλνουμε το αίτημα σε όλους τους workers
        ArrayList<Map<String, Integer>> partialResults = new ArrayList<>();
        
        for (int i = 0; i < numWorkers; i++) {
            Socket workerSocket = null;
            ObjectOutputStream outWorker = null;
            ObjectInputStream inWorker = null;
            
            try {
                String workerIP = workers[i][0];
                int workerPort = Integer.parseInt(workers[i][1]);
                
                workerSocket = new Socket(workerIP, workerPort);
                outWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                inWorker = new ObjectInputStream(workerSocket.getInputStream());
                
                // Στέλνουμε την εντολή και την κατηγορία προϊόντος
                outWorker.writeObject("productCategory");
                outWorker.flush();
                
                outWorker.writeObject(productCategory);
                outWorker.flush();
                
                // Λαμβάνουμε τα μερικά αποτελέσματα
                Map<String, Integer> workerResult = (Map<String, Integer>) inWorker.readObject();
                partialResults.add(workerResult);
                
            } finally {
                if (inWorker != null) inWorker.close();
                if (outWorker != null) outWorker.close();
                if (workerSocket != null) workerSocket.close();
            }
        }
        
        // Φάση Reduce: Συνδυάζουμε τα αποτελέσματα
        return reduceAggregationResults(partialResults);
    }
    
    /**
     * Συνδυάζει τα μερικά αποτελέσματα συγκέντρωσης από τους workers
     * @param partialResults Τα μερικά αποτελέσματα από κάθε worker
     * @return Το τελικό Map με τις συγκεντρωτικές πωλήσεις
     */
    private Map<String, Integer> reduceAggregationResults(ArrayList<Map<String, Integer>> partialResults) {
        Map<String, Integer> finalResult = new HashMap<>();
        
        // Συνδυάζουμε όλα τα μερικά αποτελέσματα
        for (Map<String, Integer> workerResult : partialResults) {
            for (Map.Entry<String, Integer> entry : workerResult.entrySet()) {
                String key = entry.getKey();
                Integer value = entry.getValue();
                
                // Προσθέτουμε την τιμή στο υπάρχον άθροισμα ή δημιουργούμε νέα καταχώρηση
                finalResult.merge(key, value, Integer::sum);
            }
        }
        
        // Υπολογίζουμε το συνολικό άθροισμα
        int total = 0;
        for (Integer value : finalResult.values()) {
            total += value;
        }
        
        // Προσθέτουμε το συνολικό άθροισμα στο αποτέλεσμα
        finalResult.put("total", total);
        
        return finalResult;
    }
}
