import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;

import java.io.PrintStream;

public interface QueryExecutor {
    void execute(Dataset dataset, Query query);
    void execute(String format, Dataset dataset, Query query, PrintStream out);
}
