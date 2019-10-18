package agg.http.data;

import org.apache.jena.graph.Triple;

import java.util.HashMap;
import java.util.Map;

/**
 * Statistics found in a page of results
 * @author Thomas Minier
 */
public class SageStatistics {
    private double suspendTime;
    private double resumeTime;
    private Map<String, Integer> cardinalities;
    private String error = "";
    private boolean done = true;
    private int next_number = 0;
    private int next_optimized_number = 0;
    private double db_size = 0;

    public SageStatistics(double suspendTime, double resumeTime) {
        this.suspendTime = suspendTime;
        this.resumeTime = resumeTime;
        this.cardinalities = new HashMap<>();
    }

    public double getDb_size() {
        return db_size;
    }

    public void setDb_size(double db_size) {
        this.db_size = db_size;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public boolean getDone() {
        return done;
    }

    public void setDone(boolean done) {
        this.done = done;
    }

    public int getNext_number() {
        return next_number;
    }

    public void setNext_number(int next_number) {
        this.next_number = next_number;
    }

    public int getNext_optimized_number() {
        return next_optimized_number;
    }

    public void setNext_optimized_number(int next_optimized_number) {
        this.next_optimized_number = next_optimized_number;
    }

    public double getSuspendTime() {
        return suspendTime;
    }

    public double getResumeTime() {
        return resumeTime;
    }

    public void addTripleCardinality(String subject, String predicate, String object, int cardinality) {
        String key = buildTripleKey(subject, predicate, object);
        if (!cardinalities.containsKey(key)) {
            cardinalities.put(key, cardinality);
        }
    }

    public boolean hasTripleCardinality(String subject, String predicate, String object) {
        String key = buildTripleKey(subject, predicate, object);
        return cardinalities.containsKey(key);
    }

    public int getCardinality(Triple triple) {
        return getCardinality(triple.getSubject().toString(), triple.getPredicate().toString(), triple.getObject().toString());
    }

    public int getCardinality (String subject, String predicate, String object) {
        String key = buildTripleKey(subject, predicate, object);
        if (cardinalities.containsKey(key)) {
            return cardinalities.get(key);
        }
        return 0;
    }

    private String buildTripleKey(String subject, String predicate, String object) {
        return "s=" + subject + ";p=" + predicate + "o=" + object;
    }
}
