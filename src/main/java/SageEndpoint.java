import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.json.simple.JSONObject;
import picocli.CommandLine;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@CommandLine.Command(name = "endpoint", footer = "Copyright(c) 2019 GRALL Arnaud",
        description = "Generate a VoID summary over a Sage Endpoint through a simple VoID description <http(s)://sage/void/>." +
                " It iterates on all Sage Datasets and create global VoID description")
public class SageEndpoint implements Runnable{
    @CommandLine.Parameters(arity = "1", description= "Sage VoID address <http(s)://domain/void>")
    public URL sageUrl = new URL("https://sage.univ-nantes.fr/void/");

    @CommandLine.Option(names="output", description = "Output directory of the generated VoID description")
    String outputLocation = "./output/";

    public static HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    HttpRequestFactory requestFactory
            = HTTP_TRANSPORT.createRequestFactory();
    public String prefixes = "PREFIX void: <http://rdfs.org/ns/void#> " +
            "PREFIX sage: <http://sage.univ-nantes.fr/sage-voc#> " +
            "PREFIX sd: <http://www.w3.org/ns/sparql-service-description#>";
    public String sageQuery = prefixes + " SELECT DISTINCT ?graph ?sparql WHERE { ?graph a sage:SageDataset . ?graph sd:endpoint ?sparql  } "; // FILTER (?graph = <http://sage.univ-nantes.fr/sparql/eventskg-r2>)
    public Model voidModel = ModelFactory.createDefaultModel();



    public SageEndpoint() throws MalformedURLException { }

    static public void main(String[] args) {
        try {
            new CommandLine(new SageEndpoint()).execute(args);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        System.err.println("Get sage void description of <" + sageUrl.toString() + ">...");
        HttpResponse response = null;

        try {
            response = requestFactory.buildGetRequest(new GenericUrl(sageUrl)).execute();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.err.println("Status code: " + response.getStatusCode());
        System.err.println("ContentType: " + response.getContentType());
        String type = response.getContentType().split("/")[1];
        RDFFormat modelFormat;
        switch (type) {
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
        System.err.println("RDFModelFormat:" + modelFormat.toString());
        System.err.println("Encoding: " + response.getContentEncoding());
        System.err.println("StatusMessage: " + response.getStatusMessage());
        System.err.println("MediaType: " + response.getMediaType());
        InputStream input = null;

        try {
            input = response.getContent();
            RDFDataMgr.read(voidModel, input, modelFormat.getLang());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // voidModel.getGraph().find().forEachRemaining(t -> System.out.println(t));
        ExecutorService executorService = Executors.newFixedThreadPool(50);
        List<Callable<Void>> callables = new ArrayList<>();

        Query query = QueryFactory.create(sageQuery);
        try (QueryExecution qexec = QueryExecutionFactory.create(query, voidModel)) {
            ResultSet results = qexec.execSelect();

            SageDataset dataset = new SageDataset();
            for (; results.hasNext(); ) {
                QuerySolution solution = results.next();

                String sageUri = solution.get("?sparql").toString();
                String graphUri = solution.get("?graph").toString();
                System.err.println("Querying: " + sageUri + " with <datasetUri> = " + graphUri);

                dataset.voidUri = graphUri;
                dataset.datasetUri = sageUri;

                callables.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        dataset.run();
                        return null;
                    }
                });


                System.err.println("Finish to query: " + sageUri + " with <datasetUri> = " + graphUri);
            }

            List<Future<Void>> futures = executorService.invokeAll(callables);
            for (Future<Void> future : futures) {
                future.get();
            }

            executorService.shutdown();

            Files.walk(new File(outputLocation).toPath())
                    .parallel()
                    .filter(Files::isDirectory).forEach(dir -> {
                System.err.println("Reading content of: " + dir);
                dataset.mergeResultFileModel(dir.toString(), voidModel);
            });
            RDFDataMgr.write(new FileOutputStream("./output/void.ttl"), voidModel, Lang.TURTLE);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
