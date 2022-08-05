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

    private static String getFileStem(String fileName) {
        int idx = fileName.lastIndexOf('.');
        return idx == -1 ? fileName : fileName.substring(0, idx);
    }

    private static File getBackupsDir() {
        String dir = System.getProperty("backup.dir");
        if (dir == null) {
            throw new RuntimeException("Backup dir not configured - set backup.dir property");
        }
        return new File(dir);
    }

    private static File getDataFileBackupDir(String fileName) {
        String stem = getFileStem(fileName);
        return new File(getBackupsDir(), stem);
    }

    private static File getDataFileRestoreDir(File backupDir) {
        // restore directory should be {backupDir}/{dbName}/{backupDate}
        // There should only be one backup date directory
        File dbDir = new File(backupDir, getTestDbName());

        if (! dbDir.exists()) {
            throw new RuntimeException(String.format("Test database backup directory %s does not exist", dbDir.getAbsolutePath()));
        }

        File[] restoreDirs = dbDir.listFiles();

        if (restoreDirs == null) {
            throw new RuntimeException(String.format("Expected %s to be a directory", dbDir.getAbsolutePath()));
        }

        if (restoreDirs.length != 1 && !restoreDirs[0].isDirectory()) {
            throw new RuntimeException(String.format("Expected single directory within backup directory %s", dbDir.getAbsolutePath()));
        }

        return restoreDirs[0];
    }

    private static Draftset findDraftset() {
        SPARQLRepository repo = getRepository();
        try(RepositoryConnection conn = repo.getConnection()) {
            String q = "PREFIX drafter: <http://publishmydata.com/def/drafter/> \n" +
                    "   SELECT ?ds WHERE { \n" +
                    "    GRAPH <http://publishmydata.com/graphs/drafter/drafts> {\n" +
                    "        ?ds a drafter:DraftSet .\n" +
                    "    }\n" +
                    "}";
            TupleQuery pq = conn.prepareTupleQuery(q);
            try(TupleQueryResult result = pq.evaluate()) {
                if (! result.hasNext()) {
                    throw new RuntimeException("Expected single draftset");
                }
                BindingSet bindings = result.next();

                if (result.hasNext()) {
                    throw new RuntimeException("Expected single draftset");
                }

                IRI draftsetIri = (IRI)bindings.getBinding("ds").getValue();
                URI draftsetUri = uri(draftsetIri.toString());

                // NOTE: java.net.URI implements DraftsetRef protocol
                return new Draftset(draftsetUri);
            }
        }
    }

    public static Draftset loadIntoDraft(File dataFile) {
        File dataBackupDir = getDataFileBackupDir(dataFile.getName());

        if (dataBackupDir.exists()) {
            // load existing backup and find draft
            File restoreDir = getDataFileRestoreDir(dataBackupDir);
            try {
                bashCommand(getStardogAdmin().getAbsolutePath(), "db", "restore", restoreDir.getAbsolutePath());
            } catch (Exception ex) {
                throw new RuntimeException(String.format("Failed to restore database for file %s", dataFile.getName()));
            }

            return findDraftset();
        } else {

            createTestDb();

            // load data file into new draft
            Drafter drafter = Drafter.create();
            Draftset draftset = drafter.createDraft(User.publisher());
            drafter.append(draftset, dataFile);

            // save backup to backups dir
            boolean created = dataBackupDir.mkdirs();
            if (! created) {
                throw new RuntimeException(String.format("Failed to create data file backups directory: %s", dataBackupDir.getAbsolutePath()));
            }

            System.out.printf("Saving to %s...%n", dataBackupDir.getAbsolutePath());

            try {
                bashCommand(getStardogAdmin().getAbsolutePath(), "db", "backup", "--to", dataBackupDir.getAbsolutePath(), getTestDbName());
            } catch (Exception ex) {
                throw new RuntimeException("Failed to create backup", ex);
            }

            return draftset;
        }
    }

    public static void main(String[] args) {
        File dataFile = resolveDataFile("data_100k_1g.nq");
        File dataBackupDir = getDataFileBackupDir(dataFile.getName());

        createTestDb();

        Drafter drafter = Drafter.create();
        Draftset draftset = drafter.createDraft(User.publisher());
        drafter.append(draftset, dataFile);

        System.out.printf("Saving to %s...%n", dataBackupDir.getAbsolutePath());

        try {
            bashCommand(getStardogAdmin().getAbsolutePath(), "db", "backup", "--to", dataBackupDir.getAbsolutePath(), getTestDbName());
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create backup", ex);
        }

        System.out.println("Dropping test db...");
        dropTestDb();
        System.out.println("Dropped!");

//        System.out.println("Restoring from backup...");
//
//        File restoreDir = getDataFileRestoreDir(dataBackupDir);
//        try {
//            bashCommand(getStardogAdmin().getAbsolutePath(), "db", "restore", restoreDir.getAbsolutePath());
//        } catch (Exception ex) {
//            throw new RuntimeException(String.format("Failed to restore database for file %s", dataFile.getName()));
//        }
//
//        System.out.println("Restored!");
    }
}
