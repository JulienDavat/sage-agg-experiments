package agg.model;

import agg.engine.iterators.SageBGPIterator;
import agg.engine.iterators.SageFilterBGPIterator;
import agg.engine.iterators.SageUnionIterator;
import agg.http.ExecutionStats;
import agg.http.SageDefaultClient;
import agg.http.SageRemoteClient;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents a remote RDF graph hosted by a Sage server
 *
 * @author Thomas Minier
 */
public class SageGraph extends GraphBase {
    private String graphURI;
    private SageRemoteClient httpClient;

    /**
     * Constructor
     *
     * @param url - URL of the dataset/graph
     */
    public SageGraph(String url) {
        super();
        graphURI = url;
        // format URL
        int index = url.lastIndexOf("/sparql/");
        String serverURL = url.substring(0, index + 7);
        this.httpClient = new SageDefaultClient(serverURL);
    }

    /**
     * Constructor
     *
     * @param url - URL of the SaGe server
     */
    public SageGraph(String url, ExecutionStats spy) {
        super();
        graphURI = url;
        // format URL
        int index = url.lastIndexOf("/sparql/");
        String serverURL = url.substring(0, index + 7);
        this.httpClient = new SageDefaultClient(serverURL, spy);
    }

    /**
     * Get the URL of the remote sage server
     *
     * @return The URL of the remote sage server
     */
    public String getGraphURI() {
        return graphURI;
    }

    /**
     * Get the HTTP client used to access the remote Sage server
     *
     * @return The HTTP client used to access the remote Sage server
     */
    public SageRemoteClient getClient() {
        return httpClient;
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triple) {
        // Jena may inject strange "ANY" that are not labelled as variable when evaluating property paths
        // so we need to sanitize the triple pattern before evaluation
        Node s = triple.getSubject();
        Node p = triple.getPredicate();
        Node o = triple.getObject();
        if (s.toString().equals("ANY")) {
            s = Var.alloc("ANY_S");
        }
        if (p.toString().equals("ANY")) {
            p = Var.alloc("ANY_P");
        }
        if (o.toString().equals("ANY")) {
            o = Var.alloc("ANY_O");
        }
        // evaluate formatted triple pattern
        Triple t = new Triple(s, p, o);
        BasicPattern bgp = new BasicPattern();
        bgp.add(t);
        QueryIterator queryIterator = new SageBGPIterator(getGraphURI(), httpClient, bgp, new LinkedHashSet<>());
        return WrappedIterator.create(queryIterator)
                .mapWith(binding -> Substitute.substitute(t, binding));
    }

    @Override
    public void close() {
        super.close();
        this.httpClient.close();
    }

    /**
     * Evaluate a Basic Graph Pattern using the SaGe server
     *
     * @param bgp - BGP to evaluate
     * @return An iterator over solution bindings for the BGP
     */
    public QueryIterator basicGraphPatternFind(BasicPattern bgp, Set<Var> vars) {
        return new SageBGPIterator(getGraphURI(), httpClient, bgp, vars);
    }

    /**
     * Evaluate a Basic Graph Pattern with a list of filters using the SaGe server
     *
     * @param bgp     - BGP to evaluate
     * @param filters - List of filters
     * @return An iterator over solution bindings for the BGP + the filters
     */
    public QueryIterator basicGraphPatternFind(BasicPattern bgp, List<Expr> filters, Set<Var> vars) {
        if (filters.isEmpty()) {
            return new SageBGPIterator(getGraphURI(), httpClient, bgp, vars);
        }
        return new SageFilterBGPIterator(getGraphURI(), httpClient, bgp, filters, vars);
    }

    /**
     * Evaluate an Union of BGPs using the SaGe server
     *
     * @param patterns - Union to evaluate
     * @return An iterator over solution bindings for the Union
     */
    public QueryIterator unionFind(List<BasicPattern> patterns) {
        return new SageUnionIterator(getGraphURI(), httpClient, patterns);
    }
}
