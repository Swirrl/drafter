package drafter.rdf;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.openrdf.http.client.SesameClient;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;

public class DrafterSPARQLRepository extends SPARQLRepository {

    private SesameClient sesameClient;
    private Integer maxConcurrentHttpConnections;

    public DrafterSPARQLRepository(String queryEndpoint) { super(queryEndpoint); }
    public DrafterSPARQLRepository(String queryEndpoint, String updateEndpoint) {
        super(queryEndpoint, updateEndpoint);
    }

    @Override public RepositoryConnection getConnection() throws RepositoryException {
        if(!this.isInitialized()) {
            throw new RepositoryException("SPARQLRepository not initialized.");
        } else {
            return new DrafterSPARQLConnection(this, createHTTPClient());
        }
    }

    @Override public synchronized SesameClient getSesameClient() {
        if (this.sesameClient == null) {
            HttpClient httpClient = newHttpClient();
            this.sesameClient = new DrafterSesameClientImpl(httpClient);
        }
        return this.sesameClient;
    }

    @Override public synchronized void setSesameClient(SesameClient client) {
        this.sesameClient = client;
    }

    /**
     * Gets the maximum number of concurrent TCP connections created per route
     * in the HTTP client used by this repository.
     * @return The maximum number of concurrent connections
     */
    public synchronized Integer getMaxConcurrentHttpConnections() {
        return this.maxConcurrentHttpConnections;
    }

    /**
     * Sets the number of concurrent TCP connections allowed per route for the
     * HTTP client used by this repository
     * @param maxConcurrentHttpConnections The maximum number of connections
     */
    public synchronized void setMaxConcurrentHttpConnections(Integer maxConcurrentHttpConnections) {
        this.maxConcurrentHttpConnections = maxConcurrentHttpConnections;

        //number of max concurrent connections might have changed so reset client so it will be re-created
        //on next usage with new maximum
        this.setSesameClient(null);
    }

    private synchronized HttpClient newHttpClient() {
        //the 'system' HTTP client uses the http.maxConnections
        //system property to define the size of its connection pool (5 is the default
        //if unset).
        //temporarily set this property to maxConcurrentHttpConnections if specified for
        //this repository
        //TODO: Construct a builder which specifies all relevant settings directly
        final String maxConnKey = "http.maxConnections";
        final String existingMax = System.getProperty(maxConnKey, "5");
        try {
            Integer maxConns = this.getMaxConcurrentHttpConnections();
            if (maxConns != null) {
                System.setProperty(maxConnKey, maxConns.toString());
            }
            return HttpClients.createSystem();
        }
        finally {
            System.setProperty(maxConnKey, existingMax);
        }
    }
}
