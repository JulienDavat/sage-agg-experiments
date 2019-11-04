package agg.engine.iterators.agg;

import agg.engine.reducers.GroupByReducer;
import agg.http.data.SolutionGroup;
import org.apache.jena.atlas.iterator.IteratorDelayedInitialization;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.expr.ExprAggregator;
import agg.engine.reducers.AggregationReducer;
import agg.http.results.QueryResults;
import agg.model.SageGraph;

import java.util.*;

public class SageGroupByIterator extends QueryIterPlainWrapper {

    public SageGroupByIterator(SageGraph graph, BasicPattern bgp, List<Var> variables, List<ExprAggregator> aggregations, VarExprList extensions, ExecutionContext exCxt) {
        super(compute(graph, bgp, variables, aggregations, extensions, exCxt));
    }

    public static Binding genKey(List<Var> variables, Binding bindings) {
        BindingHashMap b = new BindingHashMap();
        for(Var v: variables) {
            if (bindings.contains(v)) {
                b.add(v, bindings.get(v));
            } else {
                return null;
            }
        }
        return b;
    }

    private static Iterator<Binding> compute(SageGraph graph, BasicPattern bgp, List<Var> variables, List<ExprAggregator> aggregations, VarExprList extensions, ExecutionContext exCxt) {
        return new IteratorDelayedInitialization<Binding>() {

            @Override
            protected Iterator<Binding> initializeIterator() {
                System.err.println("SageGroupByIterator");
                // create reducer to gather results
                // TODO choose a GroupByReducer for unsupported query shapes
                //Reducer reducer = new GroupByReducer();
                ReducerFactory factory = new ReducerFactory(aggregations, extensions, exCxt);


                // gather all query solutions
                QueryResults results;
                Map<Binding, List<Binding>> solutions = new HashMap<>();
                boolean hasNext = true;
                Optional<String> nextLink = Optional.empty();
                int i = 0;
                GroupByReducer gpReducer = new GroupByReducer();
                while (hasNext) {
                    results = graph.getClient().queryGroupBy(graph.getGraphURI(), bgp, variables, aggregations, extensions, nextLink);
                    // regroup all bindings by key
                    for(Binding b: results.getBindings()) {
                        Binding key = genKey(variables, b);
                        if (key != null) {
                            if (!solutions.containsKey(key)) {
                                solutions.put(key, new LinkedList<>());
                            }
                            solutions.get(key).add(b);
                        }
                    }
                    for (SolutionGroup solutionGroup : results.getSolutionGroups()) {
                        gpReducer.accumulate(solutionGroup);
                    }

                    nextLink = results.getNext();
                    hasNext = results.hasNext();
                }

                // if iter1 has results. then return it, otherwise return iter2
                Iterator<Binding> iter1 = gpReducer.getGroups().parallelStream().map(entry -> {
                    Binding res = new BindingHashMap();
                    ((BindingHashMap) res).addAll(entry.keyAsBindings());

                    // build reducers for this group
                    Map<Var, AggregationReducer> reducers = factory.build();
                    entry.forEachBindings(b -> {
                        reducers.forEach((v, red) -> red.accumulate(b));
                    });

                    // build final results
                    for(Map.Entry<Var, AggregationReducer> reducer: reducers.entrySet()) {
                        Node value = reducer.getValue().getFinalValue().asNode();
                        ((BindingHashMap) res).add(reducer.getKey(), value);
                    }

                    return res;
                }).iterator();
                // produce final results from each group
                Iterator<Binding> iter2 = solutions.entrySet().parallelStream().map(entry -> {
                    Binding res = new BindingHashMap();
                    Binding key = entry.getKey();

                    // add grouping keys as results
                    ((BindingHashMap) res).addAll(key);

                    // build reducers for this group
                    Map<Var, AggregationReducer> reducers = factory.build();

                    // accumulate values into the reducers
                    for(Binding b: entry.getValue()) {
                        reducers.forEach((v, reducer) -> reducer.accumulate(b));
                    }

                    // build final results
                    for(Map.Entry<Var, AggregationReducer> reducer: reducers.entrySet()) {
                        Node value = reducer.getValue().getFinalValue().asNode();
                        ((BindingHashMap) res).add(reducer.getKey(), value);
                    }
                    return res;
                }).iterator();

                if (iter1.hasNext()) {
                    return iter1;
                } else {
                    return iter2;
                }

            }
        };
    }
}
