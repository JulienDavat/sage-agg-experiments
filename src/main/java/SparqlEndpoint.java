import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.util.Key;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

@CommandLine.Command(name = "sparql-endpoint", footer = "Copyright(c) 2019 GRALL Arnaud",
        description = "Execute a SPARQL Query on the specified SPARQL endpoint")
public class SparqlEndpoint implements Runnable {
    public static HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    @CommandLine.Parameters(index = "0", arity = "1", description = "SPARQL endpoint to query")
    String endpoint = "";
    @CommandLine.Parameters(index = "1", arity = "1", description = "The query to send to the SPARQL endpoint")
    String query = "";
    @CommandLine.Parameters(index = "2", arity = "1", description = "The default graph tu use, null by default")
    String default_graph = null;
    @CommandLine.Option(names = "--format", arity = "1", description = "The format in which the results will be returned [default = application/sparql-results+xml]")
    String format = "application/sparql-results+xml";
    HttpRequestFactory requestFactory
            = HTTP_TRANSPORT.createRequestFactory(request -> request.setReadTimeout(0));

    @Override
    public void run() {
        executeQuery(endpoint, query, default_graph, System.err, System.out, System.err, format);
    }

    public void executeQuery(String endpoint, String query, String graph, PrintStream err, PrintStream out, PrintStream stats, String format) {
        class EndpointUrl extends GenericUrl {
            @Key("default-graph-uri")
            public String default_graph_uri = null;
            @Key
            public String query;
            @Key
            public String format = "application/sparql-results+xml";

            public EndpointUrl(String encodedUrl) {
                super(encodedUrl);
            }
        }
        String queryString = query;
        EndpointUrl end = new EndpointUrl(endpoint);
        end.query = queryString;
        end.default_graph_uri = graph;
        end.format = format;
        long startTime = System.nanoTime();
        HttpRequest request;
        try {
            request = requestFactory.buildGetRequest(end);
            HttpResponse response = null;
            try {
                response = request.execute();
                long endtime = System.nanoTime();
                long elapsed = (endtime - startTime) / 1000000;
                String resp = response.parseAsString();
                String complete = "complete";
                String timeout = "notimeout";
                for (Map.Entry<String, Object> entry : response.getHeaders().entrySet()) {
//                    err.println("Key : " + entry.getKey()
//                            + " ,Value : " + entry.getValue());
                    if (entry.getKey().equals("x-sparql-maxrows")) {
                        complete = "incomplete";
                        timeout = "timeout";
                    }
                }
                out.println(resp);
                stats.println(String.join(", ", Arrays.asList(
                        "" + response.getStatusCode(),
                        "1",
                        "" + elapsed,
                        "" + resp.getBytes(StandardCharsets.UTF_8).length,
                        complete,
                        timeout
                )));
            } catch (IOException e) {
                // e.printStackTrace();
//                err.println(e);
                long endtime = System.nanoTime();
                long elapsed = (endtime - startTime) / 1000000;
                stats.println(String.join(", ", Arrays.asList(
                        "error",
                        "1",
                        "" + elapsed,
                        "0",
                        "noresult",
                        "timeout"
                )));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
