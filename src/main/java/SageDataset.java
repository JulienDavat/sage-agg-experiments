import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.gdd.sage.core.factory.SageAutoConfiguration;
import org.gdd.sage.core.factory.SageConfigurationFactory;
import org.gdd.sage.http.ExecutionStats;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import picocli.CommandLine;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@CommandLine.Command(name = "dataset", footer = "Copyright(c) 2019",
        description = "Generate a VoID summary over a Sage dataset")
public class SageDataset implements Runnable {
    @CommandLine.Parameters(arity = "1", description = "Dataset URI <...>/sparql/<...>")
    String datasetUri = null;

    @CommandLine.Parameters(description = "Name of the new void dataset generated (replace the <datasetUri> )")
    String voidUri = null;

    @CommandLine.Parameters(arity = "1", description = "Output directory of the generated VoID description")
    String outputLocation = "./output/";

    @CommandLine.Option(names = {"--time"}, description = "Display the the query execution time at the end")
    public boolean time = true;

    @CommandLine.Option(names = "process", description = "Result folder, if set this wont execute SPARQL VoID queries but will only process result.xml files to provide a void.ttl file")
    String folder = null;

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
                System.out.println("Executing the void on: " + datasetUri);

                if (voidUri == null)
                    this.voidUri = datasetUri;

                try {
                    this.voidUrl = new URL(voidUri);
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                    System.exit(1);
                }

                // load the void queries
                JSONArray queries = loadVoidQueries(datasetUri);
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
        File file = new File(System.getProperty("user.dir"), outputLocation + datasetUri.replace('/', '-').replace(':', '-'));
        System.out.println("Output dir: " + file.getAbsolutePath());
        Boolean success = file.mkdirs();
        if (success) {
            System.out.println("Successfully created the output dir to: " + file.getAbsolutePath());
        } else {
            System.out.println("Output path already exists: " + file.getAbsolutePath());
        }

        ExecutorService executorService = Executors.newFixedThreadPool(50);
        List<Callable<JSONObject>> callables = new ArrayList<>();
        queries.forEach(bucket -> {
            JSONObject buc = (JSONObject) bucket;
            String group = (String) buc.get("group");
            String description = (String) buc.get("description");
            JSONArray arr = (JSONArray) buc.get("queries");
            System.out.println("Group: " + group + " Description: " + description);

            for (Object q : arr) {
                JSONObject queryJson = (JSONObject) q;
                String query = (String) queryJson.get("query");
                String label = (String) queryJson.get("label");
                System.out.println("[" + label + "] Execute query: " + query);
                try {
                    callables.add(new Callable<JSONObject>() {
                        @Override
                        public JSONObject call() throws Exception {
                            File queryFile = new File(file.getAbsolutePath(), label + "-result.xml");
                            FileOutputStream out = null;
                            try {
                                out = new FileOutputStream(queryFile);
                            } catch (FileNotFoundException e) {
                                e.printStackTrace();
                                System.exit(1);
                            }

                            JSONObject result = new JSONObject();
                            result.put("query", queryJson);
                            try {
                                executeVoidQuery(query, new PrintStream(out));
                                result.put("response", true);
                            } catch (Exception e) {
                                result.put("response", false);
                                result.put("error", e);
                            }
                            return result;
                        }
                    });
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
    private String executeVoidQuery(String queryString, PrintStream out) {
        String type;
        Dataset federation;
        SageConfigurationFactory factory;
        ExecutionStats spy = new ExecutionStats();

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

        if (this.time) {
            double duration = spy.getExecutionTime();
            int nbQueries = spy.getNbCalls();
            System.err.println(MessageFormat.format("SPARQL query executed in {0}s with {1} HTTP requests", duration, nbQueries));
        }

        // cleanup connections
        federation.close();
        factory.close();
        return type;
    }

    /**
     * Generate VoID queries withing the dataset specified
     *
     * @param dataset the dataset where we will generate queries
     * @return
     */
    JSONArray loadVoidQueries(String dataset) {
        // load the json file
        JSONObject file = loadJSONFile("data/sportal.json");
        String uri = (String) file.get("datasetUri");
        JSONArray voID = (JSONArray) file.get("void");
        // dataset domain
        voID.forEach(bucket -> {
            JSONObject buc = (JSONObject) bucket;
            JSONArray queries = (JSONArray) buc.get("queries");
            for (Object q : queries) {
                JSONObject queryJson = (JSONObject) q;
                String query = (String) queryJson.get("query");
                // System.out.println("Replacing " + uri + " by " + this.voidUrl.toString());
                query = query.replaceAll(uri, this.voidUrl.toString());
                queryJson.put("query", query);
            }
        });
        return voID;
    }


    /**
     * Load a JSON file as a string, the file must begins by an Object (JSONObject)
     *
     * @param file
     * @return
     */
    JSONObject loadJSONFile(String file) {
        //JSON parser object to parse read file
        JSONParser jsonParser = new JSONParser();

        try (FileReader reader = new FileReader(file)) {
            //Read JSON file
            Object obj = jsonParser.parse(reader);

            JSONObject queries = (JSONObject) obj;

            return queries;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    /**
     * Determine if the string is an url
     *
     * @param s
     * @return
     */
    private boolean isURL(String s) {
        try {
            Pattern pattern = Pattern.compile("^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]");
            Matcher matcher = pattern.matcher(s);
            return matcher.matches();
        } catch (RuntimeException e) {
            return false;
        }
    }
}
