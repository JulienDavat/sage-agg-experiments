package agg.http.results;

import agg.http.data.QuerySolutions;
import agg.http.data.SageStatistics;
import agg.http.data.SolutionGroup;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.List;
import java.util.Optional;

/**
 * Results from the execution of a SPARQL query
 *
 * @author Thomas Minier
 */
public class QueryResults implements SageQueryResults {
    private QuerySolutions solutions;
    private Optional<String> next;
    private SageStatistics stats;
    private String error;
    private boolean hasError;

    public QueryResults(QuerySolutions solutions, String next, SageStatistics stats) {
        this.solutions = solutions;
        if (next == null) {
            this.next = Optional.empty();
        } else {
            this.next = Optional.of(next);
        }
        this.stats = stats;
        this.error = "No error during query evaluation";
        this.hasError = false;
    }

    private QueryResults(String error) {
        System.err.println("Error: " + error);
        this.error = error;
        this.hasError = true;
        System.exit(1);
    }

    public static QueryResults withError(String error) {
        return new QueryResults(error);
    }

    public List<Binding> getBindings() {
        return solutions.getBindings();
    }

    public List<SolutionGroup> getSolutionGroups() {
        return solutions.getGroups();
    }

    public Optional<String> getNext() {
        return next;
    }

    public SageStatistics getStats() {
        return stats;
    }

    public String getError() {
        return error;
    }

    public boolean hasError() {
        return this.hasError;
    }

    public boolean hasNext() {
        return this.next.isPresent();
    }
}
