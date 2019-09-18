package agg.http.results;

import agg.http.data.SageStatistics;

/**
 * Results obtained after executing a SPARQL query against a Sage server
 * @author Thomas Minier
 */
public interface SageQueryResults {
    SageStatistics getStats();

    String getError();

    boolean hasError();
}
