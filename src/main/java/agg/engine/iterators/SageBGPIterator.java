package agg.engine.iterators;

import org.apache.jena.sparql.core.BasicPattern;
import agg.engine.iterators.base.SageQueryIterator;
import agg.http.SageRemoteClient;
import agg.http.results.QueryResults;

import java.util.Optional;

/**
 * Evaluate a Basic Graph Pattern (BGP) using a SaGe server, following the Iterator pattern.
 * It fetches results in a lazy manner, i.e, a new HTTP request is issued only after all results
 * fetched by the previous one has been completely consumed.
 * @author Thomas Minier
 */
public class SageBGPIterator extends SageQueryIterator {

    protected BasicPattern bgp;

    /**
     * Constructor
     * @param graphURI - Default Graph URI
     * @param client - HTTP client used to query the SaGe server
     * @param bgp - Basic Graph pattern to evaluate
     */
    public SageBGPIterator(String graphURI, SageRemoteClient client, BasicPattern bgp) {
        super(graphURI, client);
        this.bgp = bgp;
    }

    @Override
    protected QueryResults query(Optional<String> nextLink) {
        return getClient().query(getGraphURI(), bgp, nextLink);
    }
}