package agg.engine.reducers;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.ExprLib;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.nodevalue.XSDFuncOp;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * A reducer used to reconstruct COUNT or SUM aggregation
 * @author Thomas Minier
 */
public class DistinctCountSumReducer extends UnaryReducer {

    public DistinctCountSumReducer(Var variable, ExecutionContext context) {
        super(variable, context);
    }

    public Set<Binding> map = new LinkedHashSet<>();

    @Override
    NodeValue bottom() {
        return NodeValue.makeInteger(0);
    }

    @Override
    NodeValue merge(NodeValue x, NodeValue y) {
        return XSDFuncOp.numAdd(x, y);
    }

    @Override
    NodeValue reduce(NodeValue v) {
        return v;
    }

    @Override
    public void accumulate(Binding bindings) {
        if (bindings.contains(variable) && !map.contains(bindings)) {
            map.add(bindings);
            // extract value
            NodeValue value = ExprLib.evalOrNull(expr, bindings, context);
            if (value != null) {
                currentValue = merge(currentValue, value);
            }
        }
    }
}
