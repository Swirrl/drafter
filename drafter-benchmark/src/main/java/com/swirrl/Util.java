package com.swirrl;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

public class Util {
    public static URI CENSUS_URI = uri("http://gss-data.org.uk/data/gss_data/census-2011-catalog-entry");

    public static void require(String nsName) {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read(nsName));
    }

    public static IFn keyword(String value) {
        return (IFn)Clojure.var("clojure.core", "keyword").invoke(value);
    }

    public static SPARQLRepository getRepository() {
        String configDbName = System.getProperty("db.name");
        String dbName = configDbName == null ? "drafter-test-db" : configDbName;
        String query = String.format("http://localhost:5820/%1$s/query", dbName);
        String update = String.format("http://localhost:5820/%1$s/update", dbName);
        return new SPARQLRepository(query, update);
    }

    public static File resolveDataFile(String fileName) {
        String dir = System.getProperty("data.dir");
        if (dir == null) {
            throw new RuntimeException("Data directory not configured - set data.dir property");
        }
        return new File(dir, fileName);
    }

    public static URI uri(String uriStr) {
        try {
            return new URI(uriStr);
        } catch (URISyntaxException ex) {
            throw new RuntimeException("Invalid URI", ex);
        }
    }

    public static Object getInputSource(File file) {
        require("drafter.rdf.sesame");
        return Clojure.var("drafter.rdf.sesame", "->FormatStatementSource").invoke(file, Util.keyword("nq"));
    }

    public static Object getInputSource(URI graph, File file) {
        require("drafter.rdf.sesame");
        return Clojure.var("drafter.rdf.sesame", "->GraphTripleStatementSource").invoke(file, graph);
    }

    private static Pattern DATA_FILE_PATTERN = Pattern.compile("^data_(\\d+)k_(\\d+)g.nq$");

    private static MatchResult matchDataFile(File dataFile) {
        Matcher m = DATA_FILE_PATTERN.matcher(dataFile.getName());
        boolean matches = m.find();

        if (! matches) {
            throw new RuntimeException(String.format("File name %1$s does not match expected data file pattern", dataFile.getName()));
        }

        return m.toMatchResult();
    }

    public static int getNumGraphs(File dataFile) {
        MatchResult r = matchDataFile(dataFile);
        return Integer.parseInt(r.group(2));
    }

    public static int getNumStatements(File dataFile) {
        MatchResult m = matchDataFile(dataFile);
        int kStatements = Integer.parseInt(m.group(1));
        return kStatements * 1000;
    }
}
