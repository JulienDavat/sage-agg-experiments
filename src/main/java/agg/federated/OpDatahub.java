package agg.federated;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

import java.util.HashMap;
import java.util.Map;

public class OpDatahub implements Op {
    private String url;
    private Map<String, BasicPattern> graphs;


    public OpDatahub(String url) {
        this.url = url;
        graphs = new HashMap<>();
    }

    public String getUrl() {
        return url;
    }

    public Map<String, BasicPattern> getGraphs() {
        return graphs;
    }

    public void addGraph(String iri, BasicPattern bgp) {
        graphs.put(iri, bgp);
    }

    @Override
    public void visit(OpVisitor opVisitor) {

    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        if (!(other instanceof OpDatahub)) {
            return false;
        }
        OpDatahub otherOp = (OpDatahub) other;
        return graphs.equals(otherOp.getGraphs());
    }

    @Override
    public String getName() {
        return "OpDatahub";
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        out.write(toString());
    }

    @Override
    public String toString(PrefixMapping pmap) {
        String out = "OpDatahub(" + url + ", [\n";
        for (Map.Entry<String, BasicPattern> graph : graphs.entrySet()) {
            out += "graph=<" + graph.getKey() + "> {" + graph.getValue().toString() + "\n";
        }
        out += "]";
        return out;
    }

    @Override
    public void output(IndentedWriter out) {
        out.write(toString());
    }
}
