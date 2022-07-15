package com.swirrl;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

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

    public static Object getInputSource(URI graph, File file) {
        require("drafter.rdf.sesame");
        return Clojure.var("drafter.rdf.sesame", "->GraphTripleStatementSource").invoke(file, graph);
    }

    public static void extractStatements(File source, Path dest, int count) throws IOException {
        int read = 0;
        try (BufferedReader r = Files.newBufferedReader(source.toPath())) {
            try (BufferedWriter w = Files.newBufferedWriter(dest)) {
                while (read < count) {
                    String line = r.readLine();
                    if (line == null) { return; }
                    w.write(line);
                    w.newLine();
                    read++;
                }
            }
        }
    }
}
