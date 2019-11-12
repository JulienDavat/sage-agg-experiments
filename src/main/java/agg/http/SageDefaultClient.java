package agg.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.ExponentialBackOff;
import org.apache.commons.io.IOUtils;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import agg.engine.update.base.UpdateQuery;
import agg.http.cache.QueryCache;
import agg.http.cache.SimpleCache;
import agg.http.data.SageQueryBuilder;
import agg.http.data.SageResponse;
import agg.http.results.QueryResults;
import agg.http.results.UpdateResults;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Allows evaluation of SPARQL queries against a SaGe server.
 * For now, only BGP and UNION queries are supported.
 * @author Thomas Minier
 */
public class SageDefaultClient implements SageRemoteClient {
    private String serverURL;
    private ExecutorService threadPool;
    private ObjectMapper mapper;
    private HttpRequestFactory requestFactory;
    private ExecutionStats spy;
    private QueryCache cache = new SimpleCache(100);
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();
    private static final String HTTP_JSON_CONTENT_TYPE = "application/json";
    // enable this when you want to use the optimized aggregator, as well as the SageOpExecutor.aggregation boolean
    public static boolean optimized = false;
    // enable this when you want to use the optimized aggregator on disk enable both
    public static boolean optimized_disk = false;

    private class JSONPayload {
        private String query;
        private String defaultGraph;
        private String next = null;
        private Boolean optimized = false;
        private Boolean optimizeddisk = false;

        public JSONPayload(String defaultGraph, String query, Boolean optimized, Boolean optimized_disk) {
            this.query = query;
            this.defaultGraph = defaultGraph;
            this.optimized = optimized;
            this.optimizeddisk = optimized_disk;
        }

        public JSONPayload(String defaultGraph, String query, String next, Boolean optimized, Boolean optimized_disk) {
            this.query = query;
            this.defaultGraph = defaultGraph;
            this.next = next;
            this.optimized = optimized;
            this.optimizeddisk = optimized_disk;
        }

        public Boolean getOptimized() { return this.optimized; }
        public void setOptimized(boolean opt) { this.optimized = opt; }
        public Boolean getOptimized_disk() { return this.optimizeddisk; }
        public void setOptimized_disk(boolean opt) { this.optimizeddisk = opt; }

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public String getDefaultGraph() {
            return defaultGraph;
        }

        public void setDefaultGraph(String defaultGraph) {
            this.defaultGraph = defaultGraph;
        }

        public String getNext() {
            return next;
        }

        public void setNext(String next) {
            this.next = next;
        }
    }

    /**
     * Constructor
     * @param serverURL - URL of the SaGe server SPARQL service, e.g.,
     */
    public SageDefaultClient(String serverURL) {
        this.serverURL = serverURL;
        threadPool = Executors.newCachedThreadPool();
        mapper = new ObjectMapper();
        requestFactory = HTTP_TRANSPORT.createRequestFactory(request -> {
            request.getHeaders().setAccept(HTTP_JSON_CONTENT_TYPE);
            request.getHeaders().setContentType(HTTP_JSON_CONTENT_TYPE);
            request.getHeaders().setUserAgent("Sage-Jena-Agg client/Java 1.8");
            request.setParser(new JsonObjectParser(JSON_FACTORY));
            // request.setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(new ExponentialBackOff()));
        });
        spy = new ExecutionStats();
    }

    /**
     * Constructor
     * @param serverURL - URL of the SaGe server
     */
    public SageDefaultClient(String serverURL, ExecutionStats spy) {
        this.serverURL = serverURL;
        threadPool = Executors.newCachedThreadPool();
        mapper = new ObjectMapper();
        requestFactory = HTTP_TRANSPORT.createRequestFactory(request -> {
            request.getHeaders().setAccept(HTTP_JSON_CONTENT_TYPE);
            request.getHeaders().setContentType(HTTP_JSON_CONTENT_TYPE);
            request.getHeaders().setUserAgent("Sage-Jena-Agg client/Java 1.8");
            request.setParser(new JsonObjectParser(JSON_FACTORY));
            // request.setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(new ExponentialBackOff()));
            request.setConnectTimeout(0);
            request.setReadTimeout(0);
        });
        this.spy = spy;
    }

    /**
     * Get the URL of the remote sage server
     * @return The URL of the remote sage server
     */
    public String getServerURL() {
        return serverURL;
    }

    private String buildJSONPayload(String graphURI, String query, Optional<String> next, Boolean optimized, Boolean optimized_disk) {
        JSONPayload payload;
        if (next.isPresent()) {
            payload = new JSONPayload(graphURI, query, next.get(), optimized, optimized_disk);
        } else {
            payload = new JSONPayload(graphURI, query, optimized, optimized_disk);
        }
        try {
            return mapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            return "";
        }
    }

    /**
     * Send a SPARQL query to the SaGe server
     * @param query - SPARQL query to send
     * @param graphURI - URI of the default graph
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    private QueryResults sendQuery(String graphURI, String query, Optional<String> next, boolean isRead) {
        // System.err.println("Sending query: " + query + " with options (optimized=" + optimized + ", disk=" + optimized_disk + ")");
        // check in cache first
        if (isRead && cache.has(graphURI, query, next)) {
            return cache.get(graphURI, query, next);
        }
        // build POST query
        GenericUrl url = new GenericUrl(serverURL);
        String payload = buildJSONPayload(graphURI, query, next, this.optimized, this.optimized_disk);
        HttpContent postContent = new ByteArrayContent(HTTP_JSON_CONTENT_TYPE, payload.getBytes());
        double startTime = System.nanoTime();
        try {
            HttpRequest request = requestFactory.buildPostRequest(url, postContent);
            HttpResponse response = request.executeAsync(threadPool).get();
            double endTime = System.nanoTime();
            if (isRead) {
                spy.reportHTTPQueryRead((endTime - startTime) / 1e9);
            } else {
                spy.reportHTTPQueryWrite((endTime - startTime) / 1e9);
            }
            return decodeResponse(response, isRead);
        } catch (InterruptedException | ExecutionException | IOException e) {
            double endTime = System.nanoTime();
            if (isRead) {
                spy.reportHTTPQueryRead((endTime - startTime) / 1e9);
            } else {
                spy.reportHTTPQueryWrite((endTime - startTime) / 1e9);
            }
            return QueryResults.withError(e.getMessage());
        }
    }

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, without a next link
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    public QueryResults query(String graphURI, BasicPattern bgp, Set<Var> projection) {
        return query(graphURI, bgp, Optional.empty(), projection);
    }

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, with a next link
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    public QueryResults query(String graphURI, BasicPattern bgp, Optional<String> next, Set<Var> projection) {
        String query = SageQueryBuilder.buildBGPQuery(bgp, projection);
        return sendQuery(graphURI, query, next, true);
    }

    /**
     * Evaluate a Basic Graph Pattern with a GROUP BY against a SaGe server, without a next link
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param variables - GROUP BY variables
     * @param aggregations - SPARQL aggregations (may be empty)
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    public QueryResults queryGroupBy(String graphURI, BasicPattern bgp, List<Var> variables, List<ExprAggregator> aggregations, VarExprList extensions) {
        return queryGroupBy(graphURI, bgp, variables, aggregations, extensions, Optional.empty());
    }

    /**
     * Evaluate a Basic Graph Pattern with a GROUP BY against a SaGe server, with a next link
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param variables - GROUP BY variables
     * @param aggregations - SPARQL aggregations (may be empty)
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    public QueryResults queryGroupBy(String graphURI, BasicPattern bgp, List<Var> variables, List<ExprAggregator> aggregations, VarExprList extensions,  Optional<String> next) {
        String query = SageQueryBuilder.buildBGPGroupByQuery(bgp, variables, aggregations, extensions);
        return sendQuery(graphURI, query, next,true);
    }

    /**
     * Evaluate a Basic Graph Pattern with filter against a SaGe server
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param filters - Filter expression
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    public QueryResults query(String graphURI, BasicPattern bgp, List<Expr> filters, Set<Var> projection) {
        return query(graphURI, bgp, filters, Optional.empty(), projection);
    }

    /**
     * Evaluate a Basic Graph Pattern with filter against a SaGe server
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param filters - Filter expressions
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    public QueryResults query(String graphURI, BasicPattern bgp, List<Expr> filters, Optional<String> next, Set<Var> projection) {
        String query = SageQueryBuilder.buildBGPQuery(bgp, filters, projection);
        return sendQuery(graphURI, query, next, true);
    }

    /**
     * Evaluate an Union of Basic Graph Patterns against a SaGe server, with a next link
     * @param graphURI - Default Graph URI
     * @param patterns - List of BGPs to evaluate
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    public QueryResults query(String graphURI, List<BasicPattern> patterns, Set<Var> projection) {
        return query(graphURI, patterns, Optional.empty(), projection);
    }

    /**
     * Evaluate an Union of Basic Graph Patterns against a SaGe server, with a next link
     * @param graphURI - Default Graph URI
     * @param patterns - List of BGPs to evaluate
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    public QueryResults query(String graphURI, List<BasicPattern> patterns, Optional<String> next, Set<Var> projection) {
        String query = SageQueryBuilder.buildUnionQuery(patterns, projection);
        return sendQuery(graphURI, query, next, true);
    }

    /**
     * Evaluate a set Graph clauses, each one wrapping a Basic Graph Patterns, against a SaGe server.
     * @param graphURI - Default Graph URI
     * @param graphs - Graphs clauses to evaluates, i..e, tuples (graph uri, basic graph pattern)
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    public QueryResults query(String graphURI, Map<String, BasicPattern> graphs, Set<Var> projection) {
        return query(graphURI, graphs, Optional.empty(), projection);
    }

    /**
     * Evaluate a set Graph clauses, each one wrapping a Basic Graph Patterns, against a SaGe server, with a next link.
     * @param graphURI - Default Graph URI
     * @param graphs - Graphs clauses to evaluates, i..e, tuples (graph uri, basic graph pattern)
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    public QueryResults query(String graphURI, Map<String, BasicPattern> graphs, Optional<String> next, Set<Var> projection) {
        String query = SageQueryBuilder.buildGraphQuery(graphs, projection);
        return sendQuery(graphURI, query, next, true);
    }

    /**
     * Evaluate a SPARQL UPDATE query using a {@link UpdateQuery} object
     * @param graphURI - Default Graph URI
     * @param query - Query to execute
     * @return Query results, containing the RDF quads that were processed by the server
     */
    public UpdateResults update(String graphURI, String query) {
        QueryResults results = sendQuery(graphURI, query, Optional.empty(), false);
        // convert QueryResults to UpdateResults
        if (results.hasError()) {
            return UpdateResults.withError(results.getError());
        }
        return new UpdateResults(graphURI, results.getBindings(), results.getStats());
    }

    /**
     * Free all resources used by the client
     */
    public void close() {
        threadPool.shutdown();
    }

    /**
     * Decode an HTTP response from a SaGe server
     * @param response - The HTTP response to decode
     * @return A decoded response
     * @throws IOException
     */
    private QueryResults decodeResponse(HttpResponse response, boolean isRead) throws IOException {
        double decodingTimeStart = System.currentTimeMillis();
        String responseContent = IOUtils.toString(response.getContent(), Charset.forName("UTF-8"));

        int statusCode = response.getStatusCode();
        if (statusCode != 200) {
            throw new IOException("Unexpected error when executing HTTP request: " + responseContent);
        }
        SageResponse sageResponse = mapper.readValue(responseContent, new TypeReference<SageResponse>(){});
        if (isRead) {
            spy.reportOverheadRead(sageResponse.stats.getResumeTime(), sageResponse.stats.getSuspendTime());
        } else {
            spy.reportOverheadWrite(sageResponse.stats.getResumeTime(), sageResponse.stats.getSuspendTime());
        }
        double decodingTimeEnd = (System.currentTimeMillis()) - decodingTimeStart;
        spy.reportDecodingResponseTime(decodingTimeEnd);
        spy.reportTransferSize(responseContent.getBytes().length);
        spy.reportNextNumbers(sageResponse.stats.getNext_number(), sageResponse.stats.getNext_optimized_number());
        spy.reportDbSize(sageResponse.stats.getDb_size());
        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss:SS");
        spy.reportLogs("[" + dateFormat.format(date) +  "]" +
                " | size(bytes): " + responseContent.getBytes().length +
                " | resume(ms): " + sageResponse.stats.getResumeTime() +
                " | suspend(ms): " + sageResponse.stats.getSuspendTime() +
                " | decoding(ms): " + decodingTimeEnd +
                " | next(normal/optimized): (" + sageResponse.stats.getNext_number() +
                    "," + sageResponse.stats.getNext_optimized_number() + ")" +
                " | db_size: " + sageResponse.stats.getDb_size()
        );
        return new QueryResults(sageResponse.bindings, sageResponse.next, sageResponse.stats);
    }
}
