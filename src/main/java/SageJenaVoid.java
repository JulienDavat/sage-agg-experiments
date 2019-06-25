import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.gdd.sage.cli.*;
import org.gdd.sage.core.factory.SageAutoConfiguration;
import org.gdd.sage.core.factory.SageConfigurationFactory;
import org.gdd.sage.http.ExecutionStats;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import picocli.CommandLine;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@CommandLine.Command(name = "ssv", footer = "Copyright(c) 2017",
        description = "Generate a VoID summary over a Sage dataset")
public class SageJenaVoid implements Runnable {
    @CommandLine.Option(names = "voidUri", description = "New URI for the new VoID graph")
    String voidUri = null;


    @CommandLine.Option(names = "dataset", description = "Dataset URI")
    String dataset = null;

    @CommandLine.Option(names = { "--format" }, description = "Results format (Result set: raw, XML, JSON, CSV, TSV; Graph: RDF serialization)")
    public String format = "xml";

    @CommandLine.Option(names = { "--time" }, description = "Display the the query execution time at the end")
    public boolean time = true;

    public static void main(String... args) {
        new CommandLine(new SageJenaVoid()).execute(args);
    }

    @Override
    public void run() {
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

    /**
     * Execute each VoID query
     * @param queries
     */
    private void executeVoidQueries(JSONArray queries) {
        queries.forEach(bucket -> {
            JSONObject buc = (JSONObject) bucket;
            String group = (String) buc.get("group");
            String description = (String) buc.get("description");
            JSONArray arr = (JSONArray) buc.get("queries");
            System.out.println("Group: " + group + " Description: " + description);
            for (Object q : arr) {
                JSONObject queryJson = (JSONObject) q;
                String query = (String) queryJson.get("query");

                System.out.println("Execute query: " + query);

                executeVoidQuery(query);
            }
        });
    }

    /**
     * Execute a SELECT VoID query over the sepecified dataset using Sage-Jena engine
     * This code is based on https://github.com/sage-org/sage-jena/blob/master/src/main/java/org/gdd/sage/cli/CLI.java
     * @param queryString
     */
    private void executeVoidQuery(String queryString) {
        try {
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

            // Evaluate SPARQL query
            QueryExecutor executor;

            if (parseQuery.isSelectType()) {
                executor = new SelectQueryExecutor(format);
            } else if (parseQuery.isAskType()) {
                executor = new AskQueryExecutor(format);
            } else if (parseQuery.isConstructType()) {
                executor = new ConstructQueryExecutor(format);
            } else {
                executor = new DescribeQueryExecutor(format);
            }
            spy.startTimer();
            executor.execute(federation, parseQuery);
            spy.stopTimer();

            // display execution time (if needed)
            if (this.time) {
                double duration = spy.getExecutionTime();
                int nbQueries = spy.getNbCalls();
                System.err.println(MessageFormat.format("SPARQL query executed in {0}s with {1} HTTP requests", duration, nbQueries));
            }

            // cleanup connections
            federation.close();
            factory.close();
        } catch(Exception e ) {
            e.printStackTrace();
            System.exit(1);
        }
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
