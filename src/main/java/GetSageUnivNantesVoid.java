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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;

public class GetSageUnivNantesVoid {
    private static HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    HttpRequestFactory requestFactory
            = HTTP_TRANSPORT.createRequestFactory();
    String prefixes = "PREFIX void: <http://rdfs.org/ns/void#> " +
            "PREFIX sage: <http://sage.univ-nantes.fr/sage-voc#> " +
            "PREFIX sd: <http://www.w3.org/ns/sparql-service-description#>";
    String sageQuery = prefixes + " SELECT DISTINCT ?graph ?sparql WHERE { ?graph a sage:SageDataset . ?graph sd:endpoint ?sparql FILTER (?graph = <http://sage.univ-nantes.fr/sparql/eventskg-r2>) } ";
    Model voidModel = ModelFactory.createDefaultModel();
    private URL sageUrl = new URL("https://sage.univ-nantes.fr/void/");

    public GetSageUnivNantesVoid() throws MalformedURLException {
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
        System.err.println("Encoding: " + response.getContentEncoding());
        System.err.println("StatusMessage: " + response.getStatusMessage());
        System.err.println("MediaType: " + response.getMediaType());
        InputStream input = null;
        try {
            input = response.getContent();
            voidModel.read(input, null, response.getContentType());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // voidModel.getGraph().find().forEachRemaining(t -> System.out.println(t));

        Query query = QueryFactory.create(sageQuery);
        try (QueryExecution qexec = QueryExecutionFactory.create(query, voidModel)) {
            ResultSet results = qexec.execSelect();

            SageJenaVoid dataset = new SageJenaVoid();
            for (; results.hasNext(); ) {
                QuerySolution solution = results.next();

                String sageUri = solution.get("?sparql").toString();
                String graphUri = solution.get("?graph").toString();
                System.err.println("Querying: " + sageUri + " with <datasetUri> = " + graphUri);

                dataset.voidUri = graphUri;
                dataset.dataset = sageUri;

                dataset.run();

                System.err.println("Finish to query: " + sageUri + " with <datasetUri> = " + graphUri);
            }


            Files.walk(new File("./output").toPath())
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

    static public void main(String[] args) {
        try {
            GetSageUnivNantesVoid sageVoid = new GetSageUnivNantesVoid();
        } catch (MalformedURLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
