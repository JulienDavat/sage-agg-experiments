package agg.http.data;

import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builder used to create SPARQL queries that can be send to a SaGe server
 *
 * @author Thomas Minier
 */
public class SageQueryBuilder {

    private SageQueryBuilder() {
    }

    private static String serializeQuery(Op root) {
        return OpAsQuery.asQuery(root).serialize();
    }

    /**
     * Build a SPARQL query from a Basic graph pattern
     *
     * @param bgp - Basic Graph pattern
     * @return Generated SPARQL query
     */
    public static String buildBGPQuery(BasicPattern bgp, Set<Var> projection) {
        return SageQueryBuilder.buildBGPQuery(bgp, new LinkedList<>(), projection.parallelStream().collect(Collectors.toList()));
    }

    /**
     * Build a SPARQL query from a Basic graph pattern
     *
     * @param bgp     - Basic Graph pattern
     * @param filters - List of SPARQL filters
     * @return Generated SPARQL query
     */
    public static String buildBGPQuery(BasicPattern bgp, List<Expr> filters, Set<Var> projection) {
        return SageQueryBuilder.buildBGPQuery(bgp, filters, projection.parallelStream().collect(Collectors.toList()));
    }

    /**
     * Build a SPARQL query from a Basic graph pattern and a list of SPARQL filters
     *
     * @param bgp       - Basic Graph pattern
     * @param filters   - List of SPARQL filters
     * @param variables - list of projection variables
     * @return Generated SPARQL query
     */
    public static String buildBGPQuery(BasicPattern bgp, List<Expr> filters, List<Var> variables) {
        // extract SPARQL variables from the BGP
        // Set<Var> variables = SageQueryBuilder.getVariables(bgp);
        // query root: the basic graph pattern itself
        Op op = new OpBGP(bgp);
        // apply SPARQL filters
        for (Expr filter : filters) {
            op = OpFilter.filter(filter, op);
        }
        // apply projection
        if (variables.size() > 0) op = new OpProject(op, variables);
        return SageQueryBuilder.serializeQuery(op);
    }

    /**
     * Build a SPARQL query from a Basic graph pattern and a list of SPARQL filters
     *
     * @param bgp       - Basic Graph pattern
     * @param variables - GROUP BY variables
     * @return Generated SPARQL query
     */
    public static String buildBGPGroupByQuery(BasicPattern bgp, List<Var> variables, List<ExprAggregator> aggregations, VarExprList extensions) {
        // query root: the basic graph pattern itself
        Op op = new OpBGP(bgp);
        // add group by
        VarExprList list = new VarExprList();
        for (Var v : variables) {
            list.add(v);
        }
        op = new OpGroup(op, list, aggregations);
        // Jena expect an aggregate query Count(?s) as ?x to have the plan:
        // OpWhere -> OpGroup(Count(?s) as ?.0) -> rename ?.0 to ?x -> project(?x)
        // so, we need to respect this format, otherwise the serialization does not work...
        op = OpExtend.create(op, extensions);
        op = new OpProject(op, extensions.getVars());
        return SageQueryBuilder.serializeQuery(op);
    }

    /**
     * Build a SPARQL query from a set of Basic graph patterns
     *
     * @param union - set of Basic Graph patterns
     * @return Generated SPARQL query
     */
    public static String buildUnionQuery(List<BasicPattern> union, Set<Var> projection) {
        // extract SPARQL variables from all BGPs
        /*Set<Var> variables = new LinkedHashSet<>();
        for(BasicPattern bgp: union) {
            variables.addAll(SageQueryBuilder.getVariables(bgp));
        }*/
        // build the union of basic graph patterns
        Op op = new OpBGP(union.get(0));
        for (int index = 1; index < union.size(); index++) {
            op = new OpUnion(op, new OpBGP(union.get(index)));
        }
        // apply projection
        op = new OpProject(op, Lists.newLinkedList(projection));
        return SageQueryBuilder.serializeQuery(op);
    }

    /**
     * Build a SPARQL query from a set of Graph clauses
     *
     * @param graphs - Set of GRAPH clauses, i.e., tuples of (graph uri, basic graph pattern)
     * @return Generated SPARQL query
     */
    public static String buildGraphQuery(Map<String, BasicPattern> graphs, Set<Var> projection) {
        Op op = null;
        for (Map.Entry<String, BasicPattern> entry : graphs.entrySet()) {
            Op opBGP = new OpBGP(entry.getValue());
            Op opGraph = new OpGraph(NodeFactory.createURI(entry.getKey()), opBGP);
            if (op == null) {
                op = opGraph;
            } else {
                op = OpJoin.create(op, opGraph);
            }
        }
        // apply projection
        op = new OpProject(op, Lists.newLinkedList(projection));
        return SageQueryBuilder.serializeQuery(op);
    }
}
