import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import picocli.CommandLine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@CommandLine.Command(name = "sportal-sparql-endpoint", description = "Execute over a SPARQL endpoint the Sportal SPARQL query workload")
public class SportalSparqlEndpoint implements Runnable {
    @CommandLine.Option(names = {"--parallel"}, description = "Parellel processing of the Sportal Workload")
    public boolean parallel = false;
    @CommandLine.Option(names = "--sportal-file", description = "Sportal file to execute")
    public String sportalFile = "./data/original-sportal.json";

    @CommandLine.Option(names = "--default-graph", description = "Default Graph to use")
    public String default_graph = null;

    @CommandLine.Parameters(index = "0", arity = "1", description = "Endpoint")
    String endpoint = "";
    @CommandLine.Parameters(index = "1", arity = "1", description = "Output directory of the response will generate two files for each query. <query>-result.xml and <query>-spy-result.txt")
    String output = "./output";

    @Override
    public void run() {
        JSONArray queries = Utils.loadVoidQueries(endpoint, sportalFile, endpoint);
        System.err.println("Sparql endpoint: " + endpoint);
        String directory = output + "/sparql-endpoint-" + endpoint.replace(":", "-").replace("/", "-");
        File dir = new File(directory);
        Boolean success = dir.mkdirs();
        if (success) {
            System.err.println("Successfully created the output dir to: " + dir.getAbsolutePath());
        } else {
            System.err.println("Output path already exists: " + dir.getAbsolutePath());
        }
        ExecutorService executorService = Executors.newFixedThreadPool(50);
        List<Callable<JSONObject>> callables = new ArrayList<>();
        queries.forEach(bucket -> {
            JSONObject buc = (JSONObject) bucket;
            String group = (String) buc.get("group");
            String description = (String) buc.get("description");
            JSONArray arr = (JSONArray) buc.get("queries");
            System.err.println("Group: " + group + " Description: " + description);

            for (Object q : arr) {
                JSONObject queryJson = (JSONObject) q;
                String query = (String) queryJson.get("query");
                String label = (String) queryJson.get("label");
                System.err.println("[" + label + "] Execute query: " + query);
                try {
                    Callable<JSONObject> callable = () -> {
                        File queryFile = new File(dir.getAbsolutePath(), label + "-result.xml");
                        File querySpy = new File(dir.getAbsolutePath(), label + "-spy-result.txt");
                        File err = new File(dir.getAbsolutePath(), label + ".log");
                        FileOutputStream outputErr = null;
                        FileOutputStream out = null;
                        FileOutputStream outSpy = null;
                        try {
                            out = new FileOutputStream(queryFile);
                            outSpy = new FileOutputStream(querySpy);
                            outputErr = new FileOutputStream(err);
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                            System.exit(1);
                        }

                        JSONObject result = new JSONObject();
                        result.put("query", queryJson);
                        try {
                            SparqlEndpoint end = new SparqlEndpoint();
                            end.endpoint = endpoint;
                            end.query = query;
                            end.default_graph = default_graph;
                            end.executeQuery(endpoint, query, default_graph, new PrintStream(outputErr), new PrintStream(out), new PrintStream(outSpy));
                            result.put("response", true);
                        } catch (Exception e) {
                            result.put("response", false);
                            result.put("error", e);
                        }
                        return result;
                    };

                    if (this.parallel) {
                        callables.add(callable);
                    } else {
                        callable.call();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    executorService.shutdown();
                    System.exit(1);
                }
            }
        });

        try {
            List<Future<JSONObject>> futures = executorService.invokeAll(callables);
            for (Future<JSONObject> future : futures) {
                future.get();
            }
            executorService.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
