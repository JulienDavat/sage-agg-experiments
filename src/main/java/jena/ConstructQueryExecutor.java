package jena;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

import java.io.PrintStream;

public class ConstructQueryExecutor extends agg.cli.ConstructQueryExecutor implements QueryExecutor {
    public ConstructQueryExecutor(String format) {
        super(format);
    }

    @Override
    public void execute(Dataset dataset, Query query) {
        super.execute(dataset, query);
    }

    public void execute(String format, Dataset dataset, Query query, PrintStream out) {
        Model resultsModel = evaluate(dataset, query);
        RDFFormat modelFormat;
        switch (format) {
            case "ttl":
            case "turtle":
            case "n3":
                modelFormat = RDFFormat.TURTLE;
                break;
            case "nt":
            case "n-triple":
            case "n-triples":
                modelFormat = RDFFormat.NTRIPLES_UTF8;
                break;
            case "json":
            case "rdf/json":
                modelFormat = RDFFormat.RDFJSON;
                break;
            case "jsonld":
                modelFormat = RDFFormat.JSONLD;
                break;
            case "thrift":
            case "rdf/binary":
                modelFormat = RDFFormat.RDF_THRIFT;
                break;
            default:
                modelFormat = RDFFormat.RDFXML;
                break;
        }
        RDFDataMgr.write(out, resultsModel, modelFormat);
    }

    @Override
    protected Model evaluate(Dataset dataset, Query query) {
        return super.evaluate(dataset, query);
    }
}
