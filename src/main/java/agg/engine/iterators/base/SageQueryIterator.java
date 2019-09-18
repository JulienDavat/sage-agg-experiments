package agg.engine.iterators.base;

import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.engine.binding.Binding;
import agg.http.SageRemoteClient;
import agg.http.results.QueryResults;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Base class used to implements Iterators that evaluate queries using a Sage server
 * @author Thomas Minier
 */
public abstract class SageQueryIterator extends BufferedIterator {
    private String graphURI;
    private SageRemoteClient client;
    protected Optional<String> nextLink;
    protected boolean hasNextPage = false;
    protected Logger logger;

    public String getGraphURI() {
        return graphURI;
    }

    public SageRemoteClient getClient() {
        return client;
    }

    /**
     * This method will be called each time the iterator needs to send another query to the server,
     * with an optional "next" link
     * @param nextLink Optional next link, used to resume query execution
     * @return Query execution results
     */
    protected abstract QueryResults query (Optional<String> nextLink);

    public SageQueryIterator(String graphURI, SageRemoteClient client) {
        this.graphURI = graphURI;
        this.client = client;
        this.nextLink = Optional.empty();
        logger = ARQ.getExecLogger();
    }

    @Override
    protected boolean canProduceBindings() {
        return hasNextPage;
    }

    @Override
    protected List<Binding> produceBindings() {
        QueryResults qr = query(nextLink);
        if (qr.hasError()) {
            return new ArrayList<>();
        }
        nextLink = qr.getNext();
        hasNextPage = qr.hasNext();
        return qr.getBindings();
    }

    @Override
    protected void closeIterator() {
        super.closeIterator();
        hasNextPage = false;
    }
}
