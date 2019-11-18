package agg.engine.reducers;

import agg.http.data.BindingsDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.expr.ExprLib;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.nodevalue.XSDFuncOp;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
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
            if (bindings.get(variable).toString().startsWith("\"[")) {
                // we need a deserialization here, as we have json serialization as array
                // also need to check if each mapping is in the map or not
                String text = bindings.get(variable).getLiteral().toString();
                text = StringEscapeUtils.unescapeJson(text);
                ObjectMapper mapper = new ObjectMapper();
                try {
                    JsonNode actualObj = mapper.readTree(text);
                    actualObj.iterator().forEachRemaining(arr -> {
                        BindingHashMap elt = new BindingHashMap();
                        arr.fields().forEachRemaining(entry -> {
                            elt.add(Var.alloc(entry.getKey().substring(1)), BindingsDeserializer.parseNode(entry.getValue().asText()));
                        });
                        // recopy information of the bindings
                        bindings.vars().forEachRemaining(var -> {
                            if (!var.equals(variable)) {
                                // System.err.println(bindings.get(var));
                                elt.add(var, bindings.get(var));
                            }
                        });
                        // now we have a binding,
                        if(!map.contains(elt)){
                            // System.err.println(elt);
                            this._accumulate(elt);
                        }
                    });
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                this._accumulate(bindings);
            }
        }
    }

    public void _accumulate(Binding bindings) {
        map.add(bindings);
        // extract value
        NodeValue value = ExprLib.evalOrNull(expr, bindings, context);
        if (value != null) {
            currentValue = merge(currentValue, value);
        }
    }
}
