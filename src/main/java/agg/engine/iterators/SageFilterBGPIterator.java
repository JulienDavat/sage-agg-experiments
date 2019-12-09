package agg.engine.iterators;

import agg.engine.iterators.base.SageQueryIterator;
import agg.http.SageRemoteClient;
import agg.http.results.QueryResults;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Like a {@link SageBGPIterator}, but also apply a set of filters on the BGP
 *
 * @author Thomas Minier
 */
public class SageFilterBGPIterator extends SageQueryIterator {
    private BasicPattern bgp;
    private List<Expr> filters;
    private Set<Var> projection;

    public SageFilterBGPIterator(String graphURI, SageRemoteClient client, BasicPattern bgp, List<Expr> filters, Set<Var> projection) {
        super(graphURI, client);
        this.bgp = bgp;
        this.filters = filters;
        this.projection = projection;
    }

    @Override
    protected QueryResults query(Optional<String> nextLink) {
        return getClient().query(getGraphURI(), bgp, filters, nextLink, projection);
    }
}
