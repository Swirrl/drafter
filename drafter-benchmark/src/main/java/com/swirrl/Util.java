package com.swirrl;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

public class Util {

    /**
     * Requires a clojure namespace
     * @param nsName The clojure namespace to require
     */
    public static void require(String nsName) {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read(nsName));
    }

    /**
     * Creates a clojure keyword from a string
     * @param value The name of the keyword
     * @return The keyword
     */
    public static IFn keyword(String value) {
        return (IFn)Clojure.var("clojure.core", "keyword").invoke(value);
    }

    private static String getTestDbName() {
        String configDbName = System.getProperty("db.name");
        return configDbName == null ? "drafter-test-db" : configDbName;
    }

    /**
     * Returns a {@link SPARQLRepository} configured to query and update the configured test database
     * @return A SPARQL repository to query and update the test database
     */
    public static SPARQLRepository getRepository() {
        String dbName = getTestDbName();
        String query = String.format("http://localhost:5820/%1$s/query", dbName);
        String update = String.format("http://localhost:5820/%1$s/update", dbName);
        return new SPARQLRepository(query, update);
    }

    /**
     * Resolves a data file name within the configured data directory
     * @param fileName The name of the file
     * @return A {@link File} representing the expected location of the data file within the data directory
     */
    public static File resolveDataFile(String fileName) {
        String dir = System.getProperty("data.dir");
        if (dir == null) {
            throw new RuntimeException("Data directory not configured - set data.dir property");
        }
        return new File(dir, fileName);
    }

    /**
     * Parses a URI string and throws a {@link RuntimeException} if the parse fails
     * @param uriStr The URI string to parse
     * @return The URI represented by the input string
     */
    public static URI uri(String uriStr) {
        try {
            return new URI(uriStr);
        } catch (URISyntaxException ex) {
            throw new RuntimeException("Invalid URI", ex);
        }
    }

    /**
     * Returns a drafter 'data source' implementation for the given NQuads data file.
     * @param file The data file
     * @return An internal data source representation for the data file
     */
    public static Object getInputSource(File file) {
        require("drafter.rdf.sesame");
        return Clojure.var("drafter.rdf.sesame", "->FormatStatementSource").invoke(file, Util.keyword("nq"));
    }

    private static Pattern DATA_FILE_PATTERN = Pattern.compile("^data_(\\d+)k_(\\d+)g_(\\d+)(pc)?.nq$");

    private static MatchResult matchDataFile(File dataFile) {
        Matcher m = DATA_FILE_PATTERN.matcher(dataFile.getName());
        boolean matches = m.find();

        if (! matches) {
            throw new RuntimeException(String.format("File name %1$s does not match expected data file pattern", dataFile.getName()));
        }

        return m.toMatchResult();
    }

    /**
     * Parses a benchmark data file name and returns the number of contained graphs
     * @param dataFile The data file to parse
     * @return The number of statements in the data file
     */
    public static int getNumGraphs(File dataFile) {
        MatchResult r = matchDataFile(dataFile);
        return Integer.parseInt(r.group(2));
    }

    /**
     * Parses a benchmark data file name and returns the number of contained statements
     * @param dataFile The data file to parse
     * @return The number of statements in the data file
     */
    public static int getNumStatements(File dataFile) {
        MatchResult m = matchDataFile(dataFile);
        int kStatements = Integer.parseInt(m.group(1));
        return kStatements * 1000;
    }

    private static File getStardogDir() {
        String dir = System.getProperty("stardog.dir");
        if (dir == null) {
            throw new RuntimeException("Stardog directory not configured - set stardog.dir property");
        }
        return new File(dir);
    }

    private static File getStardogBinDir() {
        return new File(getStardogDir(), "stardog/bin");
    }

    private static File getStardogAdmin() {
        return new File(getStardogBinDir(), "stardog-admin");
    }

    private static void dumpStream(InputStream is, String name) {
        System.out.println("Dumping " + name);

        try(BufferedReader r = new BufferedReader(new InputStreamReader(is))) {
            String line;
            while ((line = r.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void bashCommand(String... args) throws Exception {
        String cmdStr = String.join(" ", args);

        Process p = Runtime.getRuntime().exec(new String[] {"/bin/bash", "-c", cmdStr});
        //dumpStream(p.getInputStream(), "stdin");
        //dumpStream(p.getErrorStream(), "stderr");

        int exitCode = p.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Non-zero exit code when executing command");
        }
    }

    /**
     * Creates the test database using the 'stardog-admin' command
     */
    public static void createTestDb() {
        try {
            bashCommand(getStardogAdmin().getAbsolutePath(), "db", "create", "-n", getTestDbName());
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create test database", ex);
        }
    }

    /**
     * Deletes the test database using the 'stardog-admin' command
     */
    public static void dropTestDb() {
        try {
            bashCommand(getStardogAdmin().getAbsolutePath(), "db", "drop", "--", getTestDbName());
        } catch (Exception ex) {
            throw new RuntimeException("Failed to drop test database", ex);
        }
    }
}
