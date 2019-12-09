package agg.engine.update;

import agg.engine.update.base.UpdateQuery;
import agg.engine.update.queries.DeleteInsertQuery;
import agg.engine.update.queries.DeleteQuery;
import agg.engine.update.queries.InsertQuery;
import agg.http.results.UpdateResults;
import agg.model.SageGraph;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.modify.request.UpdateDataDelete;
import org.apache.jena.sparql.modify.request.UpdateDataInsert;
import org.apache.jena.sparql.modify.request.UpdateModify;
import org.apache.jena.update.Update;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.slf4j.Logger;

import java.util.LinkedList;
import java.util.List;

/**
 * Executes SPARQL UPDATE queries.
 * Transparently supports SPARQL INSERT DATA, DELETE DATA and DELETE/INSERT queries.
 *
 * @author Thomas Minier
 * @see {@href https://www.w3.org/TR/2013/REC-sparql11-update-20130321/}
 */
public class UpdateExecutor {
    private String defaultGraphURI;
    private Dataset dataset;
    private SageGraph defaultGraph;
    private int bucketSize;
    private Logger logger;

    /**
     * Constructor
     *
     * @param defaultGraphURI - URI of the default RDF Graph
     * @param dataset         - RDF dataset
     * @param bucketSize      - Bucket size, i.e., how many RDF triples to process are sent by query
     */
    public UpdateExecutor(String defaultGraphURI, Dataset dataset, int bucketSize) {
        this.defaultGraphURI = defaultGraphURI;
        this.dataset = dataset;
        // get default graph
        this.defaultGraph = (SageGraph) dataset.asDatasetGraph().getDefaultGraph();
        this.bucketSize = bucketSize;
        logger = ARQ.getExecLogger();
    }

    /**
     * Execute a SPARQL UPDATE query
     *
     * @param query - SPARQL UPDATE query
     */
    public boolean execute(String query) {
        List<UpdateQuery> updates = new LinkedList<>();

        // parse query and get all update operations in the plan
        UpdateRequest plan = UpdateFactory.create(query);

        for (Update op : plan.getOperations()) {
            if (op instanceof UpdateDataInsert) {
                UpdateDataInsert insert = (UpdateDataInsert) op;
                updates.add(new InsertQuery(insert.getQuads(), bucketSize));
            } else if (op instanceof UpdateDataDelete) {
                UpdateDataDelete delete = (UpdateDataDelete) op;
                updates.add(new DeleteQuery(delete.getQuads(), bucketSize));
            } else if (op instanceof UpdateModify) {
                UpdateModify modify = (UpdateModify) op;
                updates.add(DeleteInsertQuery.fromOperation(modify, dataset, bucketSize));
            }
        }
        // execute each update operation
        for (UpdateQuery update : updates) {
            if (!executeOne(update)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Execute an operation in a SPARQL update query
     *
     * @param update - Operation to execute
     * @return True if the execution was successfull, False otherwise
     */
    private boolean executeOne(UpdateQuery update) {
        // spin until the query has been fully executed
        while (update.hasNextQuery()) {
            // execute query using the HTTP client
            String query = update.nextQuery();
            if (query == null) {
                break;
            }
            UpdateResults results = defaultGraph.getClient().update(defaultGraphURI, query);
            // handle errors
            if (results.hasError()) {
                logger.error("Failed execution of update query: " + query);
                logger.error(results.getError());
                update.close();
                return false;
            }
            // remove quads that were processed from the update operation
            update.markAsCompleted(results.getProcessedQuads());
        }
        update.close();
        return true;
    }
}
