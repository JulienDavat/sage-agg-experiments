import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.gdd.sage.cli.*;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@CommandLine.Command(name = "ssv", footer = "Copyright(c) 2017",
        description = "Generate a VoID summary over a Sage dataset")
public class SageJenaVoid implements Runnable {
    @CommandLine.Option(names = {"--time"}, description = "Display the the query execution time at the end")
    public boolean time = true;
    @CommandLine.Option(names = "voidUri", description = "New URI for the new VoID graph")
    String voidUri = null;
    @CommandLine.Option(names = "dataset", description = "Dataset URI")
    String dataset = null;
    String format = "xml";

    @CommandLine.Option(names = "process", description = "Result folder, if set this wont execute SPARQL VoID queries but will only process result.xml files to provide a void.ttl file")
    String folder = null;



    public static void main(String... args) {
        new CommandLine(new SageJenaVoid()).execute(args);
    }

    @Override
    public void run() {
        if (folder != null) {
            mergeResultFile(folder);
        } else {
            if (dataset == null || voidUri == null) {
                CommandLine.usage(this, System.out);
            } else {
                System.out.println("New VoID graph URI: " + voidUri);
                System.out.println("Executing the void on: " + dataset);

                // load the void queries
                JSONArray queries = loadVoidQueries(dataset);
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
        URL voidUriURL = null;
        try {
            voidUriURL = new URL(voidUri);
        } catch (MalformedURLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        String path = voidUriURL.getHost() + voidUriURL.getPath().replace('/', '_');
        File file = new File(System.getProperty("user.dir"), path);
        System.out.println("Output dir: " + file.getAbsolutePath());
        Boolean success = file.mkdirs();
        if (success) {
            System.out.println("Successfully created the output dir to: " + file.getAbsolutePath());
        } else {
            System.out.println("Output path already exists: " + file.getAbsolutePath());
        }

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

                File queryFile = new File(file.getAbsolutePath(), label + "-result.xml");
                FileOutputStream out = null;
                try {
                    out = new FileOutputStream(queryFile);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                    System.exit(1);
                }

                System.out.println("[" + label + "] Execute query: " + query);

                try {
                    String type = executeVoidQuery(query, new PrintStream(out));
                    queryJson.put("type", type);
                    queryJson.put("response", true);
                } catch (Exception e) {
                    e.printStackTrace();
                    queryJson.put("response", false);
                }
            }
        });

        // write the json modified into the
        File jsonOutput = new File(file.getAbsolutePath(), "queries.json");
        try {
            queries.writeJSONString(new FileWriter(jsonOutput));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Read all result files then output the summury into a void.ttl file in the generated output folder
     * The generated folder will contain all results as well as the json file used to generate results.
     */
    private void mergeResultFile(String folder) {
        File dir = new File(folder);
        File output = new File(dir.getAbsolutePath(), "void.ttl");
        File[] listOfFiles = dir.listFiles();
        System.out.println(dir.getAbsolutePath());

        Model m = ModelFactory.createDefaultModel();
        for (File file : listOfFiles) {
            if(file.getAbsolutePath().contains(".xml") && !file.getAbsolutePath().contains("QA")) {
                System.out.println("Processing: " + file.getAbsolutePath());
                Model tmp = ModelFactory.createDefaultModel();
                try {
                    tmp.read(new FileInputStream(file.getAbsoluteFile()), null, "RDFXML");
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                m.add(tmp);
            }
        }

        try {
            m.write(new FileOutputStream(output), "TURTLE");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
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
        factory = new SageAutoConfiguration(dataset, parseQuery, spy);

        // Init Sage dataset (maybe federated)
        factory.configure();
        factory.buildDataset();
        parseQuery = factory.getQuery();
        federation = factory.getDataset();

        PrintStream originalOut = System.out;
        System.setOut(out);


        // Evaluate SPARQL query
        QueryExecutor executor;

        if (parseQuery.isSelectType()) {
            executor = new SelectQueryExecutor(format);
            type = "select";
        } else if (parseQuery.isAskType()) {
            executor = new AskQueryExecutor(format);
            type = "ask";
        } else if (parseQuery.isConstructType()) {
            executor = new ConstructQueryExecutor(format);
            type = "construct";
        } else {
            executor = new DescribeQueryExecutor(format);
            type = "describe";
        }
        spy.startTimer();
        executor.execute(federation, parseQuery);
        spy.stopTimer();

        if (this.time) {
            double duration = spy.getExecutionTime();
            int nbQueries = spy.getNbCalls();
            System.err.println(MessageFormat.format("SPARQL query executed in {0}s with {1} HTTP requests", duration, nbQueries));
        }

        // cleanup connections
        federation.close();
        factory.close();
        System.setOut(originalOut);
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
        JSONArray newVoID = new JSONArray();
        // dataset domain
        voID.forEach(bucket -> {
            JSONObject buc = (JSONObject) bucket;
            String group = (String) buc.get("group");
            String description = (String) buc.get("description");
            JSONArray queries = (JSONArray) buc.get("queries");
            // System.out.println("Group: " + group);
            // System.out.println("Description: " + description);
            for (Object q : queries) {
                JSONObject queryJson = (JSONObject) q;
                String query = (String) queryJson.get("query");
                query = query.replaceAll(uri, voidUri);
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
