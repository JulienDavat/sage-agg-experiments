package jena;

import org.apache.jena.query.*;

import java.io.PrintStream;

public class SelectQueryExecutor extends org.gdd.sage.cli.SelectQueryExecutor implements QueryExecutor {
    public SelectQueryExecutor(String format) {
        super(format);
    }

    @Override
    public void execute(Dataset dataset, Query query) {
        super.execute(dataset, query);
    }

    public void execute(String format, Dataset dataset, Query query, PrintStream out) {
        if (query.isSelectType()) {
            try (QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
                ResultSet results = qexec.execSelect();
                switch (format) {
                    case "raw":
                        results.forEachRemaining(out::print);
                        break;
                    case "xml":
                        ResultSetFormatter.outputAsXML(out, results);
                        break;
                    case "json":
                        ResultSetFormatter.outputAsJSON(out, results);
                        break;
                    case "csv":
                        ResultSetFormatter.outputAsCSV(out, results);
                        break;
                    case "tsv":
                        ResultSetFormatter.outputAsTSV(out, results);
                        break;
                    default:
                        ResultSetFormatter.outputAsSSE(out, results);
                        break;
                }
            }
        }
    }
}
