import agg.engine.SageOpExecutor;
import jena.ConstructQueryExecutor;
import jena.QueryExecutor;
import jena.SelectQueryExecutor;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import agg.core.factory.SageAutoConfiguration;
import agg.core.factory.SageConfigurationFactory;
import agg.http.ExecutionStats;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import picocli.CommandLine;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

@CommandLine.Command(name = "dataset", footer = "Copyright(c) 2019 GRALL Arnaud",
        description = "Generate a VoID summary over a Sage dataset using the sage-agg server")
public class SageDataset implements Runnable {
    @CommandLine.Parameters(index = "0", arity = "1", description = "Dataset URI <...>/sparql/<...>")
    public String datasetUri = null;

    @CommandLine.Parameters(index = "1", arity = "1", description = "Name of the new void dataset generated (replace the <datasetUri> )")
    public String voidUri = null;

    @CommandLine.Parameters(index = "2", arity = "1", description = "Output directory of the generated VoID description")
    public String outputLocation = "./output/";

    @CommandLine.Option(names = {"--time"}, description = "Display the the query execution time at the end")
    public boolean time = false;

    @CommandLine.Option(names = "process", description = "Result folder, if set this wont execute SPARQL VoID queries but will only process result.xml files to provide a void.ttl file")
    public String folder = null;

    @CommandLine.Option(names = {"--parallel"}, description = "Parallel processing of the Sportal Workload")
    public boolean parallel = false;

    @CommandLine.Option(names = {"--sportal-file"}, description = "Specify the location of the sportal file wich will be executed")
    public String sportalFile = "data/original-sportal.json";

    @CommandLine.Option(names = {"--optimized"}, description = "Enable the aggregation optimization")
    public Boolean optimized = false;
    
    private String format = "xml";
    private URL voidUrl = null;

    public static void main(String... args) {
        new CommandLine(new SageDataset()).execute(args);
    }

    @Override
    public void run() {
        if (folder != null) {
            mergeResultFile(folder, null);
        } else {
            if (datasetUri == null) {
                CommandLine.usage(this, System.out);
            } else {
                System.err.println("Executing the void on: " + datasetUri);

                if (voidUri == null)
                    this.voidUri = datasetUri;

                try {
                    this.voidUrl = new URL(voidUri);
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                    System.exit(1);
                }

                // load the void queries
                JSONArray queries = Utils.loadVoidQueries(datasetUri, sportalFile, voidUrl.toString());
                // Now execute queries by group
                executeVoidQueries(queries);
            }
        }

    }

    /**
     * Execute each VoID query
     *
     * @param queries
     */
    private void executeVoidQueries(JSONArray queries) {
        // create the result dir
        SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy-HH-mm-ss");
        Date date = new Date();
        String stringdate = formatter.format(date);
        File file = new File(System.getProperty("user.dir"), outputLocation + "sage-" + stringdate + "-" + datasetUri.replace('/', '-').replace(':', '-'));
        System.err.println("Output dir: " + file.getAbsolutePath());
        Boolean success = file.mkdirs();
        if (success) {
            System.err.println("Successfully created the output dir to: " + file.getAbsolutePath());
        } else {
            System.err.println("Output path already exists: " + file.getAbsolutePath());
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
                    Callable<JSONObject> callable = new Callable<JSONObject>() {
                        @Override
                        public JSONObject call() throws Exception {
                            File queryFile = new File(file.getAbsolutePath(), label + "-result.xml");
                            File querySpy = new File(file.getAbsolutePath(), label + "-spy-result.txt");
                            FileOutputStream out = null;
                            FileOutputStream outSpy = null;
                            try {
                                out = new FileOutputStream(queryFile);
                                outSpy = new FileOutputStream(querySpy);
                            } catch (FileNotFoundException e) {
                                e.printStackTrace();
                                System.exit(1);
                            }

                            JSONObject result = new JSONObject();
                            result.put("query", queryJson);
                            try {
                                executeVoidQuery(label, query, new PrintStream(out), new PrintStream(outSpy), time);
                                result.put("response", true);
                            } catch (Exception e) {
                                System.err.println(e);
                                result.put("response", false);
                                result.put("error", e);
                            }
                            return result;
                        }
                    };

                    if (this.parallel) {
                        callables.add(callable);
                    } else {
                        callable.call();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
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

    /**
     * Read all result files then output the summury into a void.ttl file in the generated output folder
     * The generated folder will contain all results as well as the json file used to generate results.
     */
    public void mergeResultFile(String folder, Model m) {
        File dir = new File(folder);
        Model model = mergeResultFileModel(folder, m);
        File output = new File(dir.getAbsolutePath(), "void.ttl");
        try {
            model.write(new FileOutputStream(output), "TURTLE");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Read all result files then output the summury into a void.ttl file in the generated output folder
     * The generated folder will contain all results as well as the json file used to generate results.
     */
    public Model mergeResultFileModel(String folder, Model m) {
        File dir = new File(folder);
        File[] listOfFiles = dir.listFiles();

        if (m == null) {
            ModelFactory.createDefaultModel();
        }
        for (File file : listOfFiles) {
            if (file.getAbsolutePath().contains(".xml") && !file.getAbsolutePath().contains("QA")) {
                System.err.println("Processing: " + file.getAbsolutePath());
                Model tmp = ModelFactory.createDefaultModel();
                try {
                    tmp.read(new FileInputStream(file.getAbsoluteFile()), this.voidUrl.toString(), "RDFXML");
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                m.add(tmp);
            }
        }

        return m;
    }

    /**
     * Execute a SELECT VoID query over the sepecified dataset using Sage-Jena engine
     * This code is based on https://github.com/sage-org/sage-jena/blob/master/src/main/java/org/gdd/sage/cli/CLI.java
     *
     * @param queryString
     */
    private String executeVoidQuery(String label, String queryString, PrintStream out, PrintStream spyOut, boolean time) {
        try {
            String type;
            Dataset federation;
            SageConfigurationFactory factory;
            ExecutionStats spy = new ExecutionStats();
            if (this.time) {
                spy.setLogs(true);
            }

            if (this.optimized) SageOpExecutor.aggregations = this.optimized;

            Query parseQuery = QueryFactory.create(queryString);
            factory = new SageAutoConfiguration(datasetUri, parseQuery, spy);
            // Init Sage dataset (maybe federated)
            factory.configure();
            factory.buildDataset();
            parseQuery = factory.getQuery();
            federation = factory.getDataset();
            // Evaluate SPARQL query
            QueryExecutor executor;

            if (parseQuery.isSelectType()) {
                executor = new SelectQueryExecutor(format);
                type = "select";
            } else if (parseQuery.isConstructType()) {
                executor = new ConstructQueryExecutor(format);
                type = "construct";
            } else {
                throw new Error("Cannot parse the query: we only perform select or construct for generating voids");
            }
            spy.startTimer();
            executor.execute(format, federation, parseQuery, out);
            spy.stopTimer();

            double duration = spy.getExecutionTime();
            int nbQueries = spy.getNbCallsRead();
            double transferSize = spy.getTotalTransferSize();
            if (time) System.err.println(MessageFormat.format("[" + label + "] SPARQL query executed in {0}s with {1} HTTP requests with {2} Bytes received", duration, nbQueries, transferSize));
            spyOut.println(String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s",
                    duration, spy.getNbCallsRead(), spy.getNbCallsWrite(),
                    spy.getMeanHTTPTimesRead(), spy.getMeanHTTPTimesWrite(),
                    spy.getMeanResumeTimeRead(), spy.getMeanResumeTimeWrite(),
                    spy.getMeanSuspendTimeRead(), spy.getMeanSuspendTimeWrite(),
                    spy.getMeanTransferSize()));

            // cleanup connections
            federation.close();
            factory.close();
            return type;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
