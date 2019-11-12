package agg.engine;

import com.google.common.collect.Sets;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprList;
import agg.core.SageUtils;
import agg.engine.iterators.boundjoin.ParallelBoundJoinIterator;
import agg.model.SageGraph;
import org.apache.jena.sparql.graph.NodeTransform;
import org.apache.jena.sparql.util.Symbol;

import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 * Provides a custom StageGenerator for evaluating SPARQL queries against a SaGe server
 * @author Thomas Minier
 */
public class SageStageGenerator implements StageGenerator {
    private final ExecutorService threadPool;
    private StageGenerator above;
    private static final int BIND_JOIN_BUCKET_SIZE = 15;

    private SageStageGenerator(StageGenerator above, ExecutorService threadPool) {
        this.above = above;
        this.threadPool = threadPool;
    }

    /**
     * Factory method used to create a SageStageGenerator with the default ARQ context
     * @return A SageStageGenerator configured with the default ARQ context
     */
    public static SageStageGenerator createDefault(ExecutorService threadPool) {
        StageGenerator orig = ARQ.getContext().get(ARQ.stageGenerator);
        return new SageStageGenerator(orig, threadPool);
    }

    @Override
    public QueryIterator execute(BasicPattern pattern,
                                 QueryIterator input,
                                 ExecutionContext execCxt) {
        Graph g = execCxt.getActiveGraph();

        Set<Var> varSet = this.getVarSetFromContext(execCxt);

        // This stage generator only support evaluation of a Sage Graph
        if (g instanceof SageGraph) {
            SageGraph sageGraph = (SageGraph) g;

            // no input bindings => simply evaluate the BGP
            if (input.isJoinIdentity()) {
                return sageGraph.basicGraphPatternFind(pattern, varSet);
            }

            // if we can download the right pattern in one call, use a hash join instead of a bound join
            /*QueryResults rightRes = sageGraph.getClient().query(pattern);
            if (!rightRes.hasNext()) {
                QueryIterator rightIter = new QueryIterPlainWrapper(rightRes.getBindings().iterator());
                return QueryIterHashJoin.create(input, rightIter, execCxt);
            }*/
            // otherwise, use a bind join
            return new ParallelBoundJoinIterator(input, sageGraph.getGraphURI(), sageGraph.getClient(), pattern, threadPool, BIND_JOIN_BUCKET_SIZE, varSet);
        }

        // delegate execution of the unsupported Graph to the StageGenerator above
        return above.execute(pattern, input, execCxt);
    }

    private Set<Var> getVarSetFromContext(ExecutionContext execCxt) {
        Op alg = execCxt.getContext().get(Symbol.create("http://jena.apache.org/ARQ/system#algebra"));
        Query query = OpAsQuery.asQuery(alg);
        Set<Var> varSet = new LinkedHashSet<>();
        Set<Var> varAggSet = new LinkedHashSet<>();
        Set<Var> varAggSetBis = new LinkedHashSet<>();
        query.getAggregators().forEach((exprAggregator -> {
            varAggSetBis.add(exprAggregator.getVar());
            if (exprAggregator.getAggregator().getExprList() != null) {
                for (Expr expr : exprAggregator.getAggregator().getExprList()) {
                    varAggSet.add(expr.asVar());
                }
            }
        }));
        for (Var var : query.getProject().getVars()) {
            Expr exp = query.getProject().getExpr(var);
            if (exp == null) {
                varSet.add(var);
            } else {
                // is the variable bind to an aggregator? yes if not any of them
                if (exp.isConstant() || exp.isFunction() || exp.isVariable()) {
                    varSet.add(var);
                }
            }
        }
        varSet.addAll(varAggSet);
        return varSet;
    }

    public QueryIterator execute(BasicPattern pattern,
                                 QueryIterator input,
                                 ExecutionContext execCxt,
                                 ExprList filters) {
        Graph g = execCxt.getActiveGraph();

        Set<Var> varSet = this.getVarSetFromContext(execCxt);

        if (g instanceof SageGraph) {
            SageGraph sageGraph = (SageGraph) g;

            // no input bindings => simply evaluate the BGP
            if (input.isJoinIdentity()) {
                // compute which filters can be packed with the BGP
                return sageGraph.basicGraphPatternFind(pattern, findRelevantFilters(filters, pattern), varSet);
            }

            // otherwise, use a bind join
            return new ParallelBoundJoinIterator(input, sageGraph.getGraphURI(), sageGraph.getClient(), pattern, threadPool, BIND_JOIN_BUCKET_SIZE, varSet);
        }
        return above.execute(pattern, input, execCxt);
    }

    /**
     * Find all filters that can be applied to a Basic Graph Pattern
     * @param filters - List of filters to analyze
     * @param bgp - Basic graph pattern
     * @return The list of all filters that can be applied to the Basic Graph Pattern
     */
    private List<Expr> findRelevantFilters(ExprList filters, BasicPattern bgp) {
        List<Expr> relevantFilters = new LinkedList<>();
        Set<Var> bgpVariables = SageUtils.getVariables(bgp);
        for(Expr filter: filters.getList()) {
            Set<Var> filterVariables = filter.getVarsMentioned();
            // test if filterVariables is a subset of bgpVariables, i.e., filterVariables - bgpVariables = empty set
            if(Sets.difference(filterVariables, bgpVariables).isEmpty()) {
                relevantFilters.add(filter);
            }
        }
        System.out.println(relevantFilters);
        return relevantFilters;
    }
}
