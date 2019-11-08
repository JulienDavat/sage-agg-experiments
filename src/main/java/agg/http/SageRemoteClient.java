package agg.http;

import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import agg.engine.update.base.UpdateQuery;
import agg.http.results.QueryResults;
import agg.http.results.UpdateResults;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Generic interface for an HTTP client that sends SPARQL queries to a Sage server.
 * @author Thomas Minier
 */
public interface SageRemoteClient {
    /**
     * Get the URL of the remote sage server
     * @return The URL of the remote sage server
     */
    String getServerURL();

    /**
     * Free all resources used by the client
     */
    void close();

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, without a next link
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param projection - Set of projection variables
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    QueryResults query(String graphURI, BasicPattern bgp, Set<Var> projection);

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, with a next link
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param next - Optional link used to resume query evaluation
     * @param projection - Set of projection variables
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    QueryResults query(String graphURI, BasicPattern bgp, Optional<String> next, Set<Var> projection);

    /**
     * Evaluate a Basic Graph Pattern with a GROUP BY against a SaGe server, without a next link
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param variables - GROUP BY variables
     * @param aggregations - SPARQL aggregations (may be empty)
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    QueryResults queryGroupBy(String graphURI, BasicPattern bgp, List<Var> variables, List<ExprAggregator> aggregations, VarExprList extensions);

    /**
     * Evaluate a Basic Graph Pattern with a GROUP BY against a SaGe server, with a next link
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param variables - GROUP BY variables
     * @param aggregations - SPARQL aggregations (may be empty)
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    QueryResults queryGroupBy(String graphURI, BasicPattern bgp, List<Var> variables, List<ExprAggregator> aggregations, VarExprList extensions, Optional<String> next);

    /**
     * Evaluate a Basic Graph Pattern with filter against a SaGe server
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param filters - Filter expression
     * @param projection - Set of projection variables
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    QueryResults query(String graphURI, BasicPattern bgp, List<Expr> filters, Set<Var> projection);

    /**
     * Evaluate a Basic Graph Pattern with filter against a SaGe server
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param filters - Filter expressions
     * @param next - Optional link used to resume query evaluation
     * @param projection - Set of projection variables
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    QueryResults query(String graphURI, BasicPattern bgp, List<Expr> filters, Optional<String> next, Set<Var> projection);

    /**
     * Evaluate an Union of Basic Graph Patterns against a SaGe server, with a next link
     * @param graphURI - Default Graph URI
     * @param patterns - List of BGPs to evaluate
     * @param projection - Set of projection variables
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    QueryResults query(String graphURI, List<BasicPattern> patterns, Set<Var> projection);

    /**
     * Evaluate an Union of Basic Graph Patterns against a SaGe server, with a next link
     * @param graphURI - Default Graph URI
     * @param patterns - List of BGPs to evaluate
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    QueryResults query(String graphURI, List<BasicPattern> patterns, Optional<String> next, Set<Var> projection);

    /**
     * Evaluate a set Graph clauses, each one wrapping a Basic Graph Patterns, against a SaGe server.
     * @param graphURI - Default Graph URI
     * @param graphs - Graphs clauses to evaluates, i..e, tuples (graph uri, basic graph pattern)
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    QueryResults query(String graphURI, Map<String, BasicPattern> graphs, Set<Var> projection);

    /**
     * Evaluate a set Graph clauses, each one wrapping a Basic Graph Patterns, against a SaGe server, with a next link.
     * @param graphURI - Default Graph URI
     * @param graphs - Graphs clauses to evaluates, i..e, tuples (graph uri, basic graph pattern)
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    QueryResults query(String graphURI, Map<String, BasicPattern> graphs, Optional<String> next, Set<Var> projection);

    /**
     * Evaluate a SPARQL UPDATE query using a {@link UpdateQuery} object
     * @param graphURI - Default Graph URI
     * @param query - Query to execute
     * @return Query results, containing the RDF quads that were processed by the server
     */
    UpdateResults update(String graphURI, String query);
}
