import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.util.Key;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

@CommandLine.Command(name = "sparql-endpoint", footer = "Copyright(c) 2019 GRALL Arnaud",
        description = "Execute a SPARQL Query on the specified SPARQL endpoint")
public class SparqlEndpoint implements Runnable {
    public static HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    @CommandLine.Parameters(index = "0", arity = "1", description = "SPARQL endpoint to query")
    String endpoint = "";
    @CommandLine.Parameters(index = "1", arity = "1", description = "The query to send to the SPARQL endpoint")
    String query = "";
    HttpRequestFactory requestFactory
            = HTTP_TRANSPORT.createRequestFactory(request -> request.setReadTimeout(0));

    @Override
    public void run() {
        executeQuery(endpoint, query, System.err, System.out, System.out);
    }

    public void executeQuery(String endpoint, String query, PrintStream err, PrintStream out, PrintStream stats) {
        class EndpointUrl extends GenericUrl {
            @Key
            public String query;

            public EndpointUrl(String encodedUrl) {
                super(encodedUrl);
            }
        }
        String queryString = query;
        System.err.println("SPARQL endpoint: " + endpoint);
        System.err.println("SPARQL query: " + queryString);

        EndpointUrl end = new EndpointUrl(endpoint);
        end.query = queryString;
        long startTime = System.nanoTime();
        HttpRequest request;
        try {
            request = requestFactory.buildGetRequest(end);
            HttpResponse response = null;
            try {
                HttpHeaders header = request.getHeaders();
                header.set("Accept", "application/sparql-results+xml, application/rdf+xml");
                request.setHeaders(header);
                response = request.execute();

                long endtime = System.nanoTime();
                long elapsed = (endtime - startTime) / 1000000;
                err.println("Execution time: " + elapsed);
                err.println("Status Code: " + response.getStatusCode());
                err.println("Content Type: " + response.getContentType());
                err.println("Content Encoding: " + response.getContentEncoding());
                err.println("Content Type: " + response.getContentType());
                String resp = response.parseAsString();
                err.println("Size: " + resp.getBytes("UTF-8").length + " bytes");
                String complete = "complete";
                String timeout = "notimeout";
                for (Map.Entry<String, Object> entry : response.getHeaders().entrySet()) {
                    err.println("Key : " + entry.getKey()
                            + " ,Value : " + entry.getValue());
                    if (entry.getKey().equals("x-sparql-maxrows")) {
                        complete = "incomplete timeout";
                    }
                }
                out.println(resp);
                stats.println(response.getStatusCode() + " 1 " + elapsed + " " + resp.getBytes("UTF-8").length + " " + complete + " " + timeout);
            } catch (IOException e) {
                e.printStackTrace();
                err.println(e);
                long endtime = System.nanoTime();
                long elapsed = (endtime - startTime) / 1000000;
                stats.println("error 1 " + elapsed + "  0 noresult timeout");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
