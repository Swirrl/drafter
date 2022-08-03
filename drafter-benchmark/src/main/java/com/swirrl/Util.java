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

    public static void require(String nsName) {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read(nsName));
    }

    public static IFn keyword(String value) {
        return (IFn)Clojure.var("clojure.core", "keyword").invoke(value);
    }

    private static String getTestDbName() {
        String configDbName = System.getProperty("db.name");
        return configDbName == null ? "drafter-test-db" : configDbName;
    }

    public static SPARQLRepository getRepository() {
        String dbName = getTestDbName();
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

    private static void bashCommand(String... args) throws Exception {
        String[] procArgs = new String[args.length + 2];
        procArgs[0] = "bash";
        procArgs[1] = "-c";
        for (int i = 0; i < args.length; ++i) {
            procArgs[i + 2] = args[i];
        }

        Process p = Runtime.getRuntime().exec(procArgs);
        int exitCode = p.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Non-zero exit code when executing command");
        }
    }

    public static void createTestDb() {
        try {
            bashCommand(getStardogAdmin().getAbsolutePath(), "db", "create", "-n", getTestDbName());
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create test database", ex);
        }
    }

    public static void dropTestDb() {
        try {
            bashCommand(getStardogAdmin().getAbsolutePath(), "db", "drop", "--", getTestDbName());
        } catch (Exception ex) {
            throw new RuntimeException("Failed to drop test database", ex);
        }
    }
}
