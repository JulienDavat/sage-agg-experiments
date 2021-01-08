import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    /**
     * Generate VoID queries withing the dataset specified
     *
     * @param dataset the dataset where we will generate queries
     * @return
     */
    public static JSONArray loadVoidQueries(String dataset, String voidFile, String voidReplace) {
        // load the json file
        JSONObject file = loadJSONFile(voidFile);
        String uri = (String) file.get("datasetUri");
        JSONArray voID = (JSONArray) file.get("void");
        // dataset domain
        voID.forEach(bucket -> {
            JSONObject buc = (JSONObject) bucket;
            JSONArray queries = (JSONArray) buc.get("queries");
            for (Object q : queries) {
                JSONObject queryJson = (JSONObject) q;
                String query = (String) queryJson.get("query");
                // System.err.println("Replacing " + uri + " by " + this.voidUrl.toString());
                query = query.replaceAll(uri, voidReplace);
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
    public static JSONObject loadJSONFile(String file) {
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
