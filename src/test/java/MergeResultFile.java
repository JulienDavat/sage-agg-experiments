import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class MergeResultFile {

    @Test
    public void merge() throws FileNotFoundException {
        File dir = new File(System.getProperty("user.dir"), "sage.univ-nantes.fr_void_swdf-2012");
        File output = new File(dir.getAbsolutePath(), "void.ttl");
        File[] listOfFiles = dir.listFiles();
        System.out.println(dir.getAbsolutePath());

        Model m = ModelFactory.createDefaultModel();
        for (File file : listOfFiles) {
            if(file.getAbsolutePath().contains(".xml") && !file.getAbsolutePath().contains("QA")) {
                Model tmp = ModelFactory.createDefaultModel();
                tmp.read(new FileInputStream(file.getAbsoluteFile()), null, "RDFXML");
                m.add(tmp);
            }
        }

        m.write(new FileOutputStream(output), "TURTLE");
    }
}
