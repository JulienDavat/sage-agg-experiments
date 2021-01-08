package agg.engine.iterators;

import agg.engine.iterators.base.SageQueryIterator;
import agg.http.SageRemoteClient;
import agg.http.results.QueryResults;
import org.apache.jena.sparql.core.BasicPattern;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

/**
 * An Iterator that evaluates a Union query query using a Sage server
 *
 * @author Thomas Minier
 */
public class SageUnionIterator extends SageQueryIterator {

    private List<BasicPattern> patterns;

    public SageUnionIterator(String graphURI, SageRemoteClient client, List<BasicPattern> patterns) {
        super(graphURI, client);
        this.patterns = patterns;
    }

    @Override
    protected QueryResults query(Optional<String> nextLink) {
        return getClient().query(getGraphURI(), patterns, nextLink, new LinkedHashSet<>());
    }
}
