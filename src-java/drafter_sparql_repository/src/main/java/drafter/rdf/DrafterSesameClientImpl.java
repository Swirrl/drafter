package drafter.rdf;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.eclipse.rdf4j.http.client.SesameClientImpl;
import org.eclipse.rdf4j.http.client.SparqlSession;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DrafterSesameClientImpl extends SesameClientImpl {

    private static final ExecutorService QUERY_EXECUTOR = Executors.newCachedThreadPool();

    public DrafterSesameClientImpl(HttpClient httpClient) {
        this.setHttpClient(httpClient);
    }

    @Override public synchronized SparqlSession createSparqlSession(String queryEndpointUrl, String updateEndpointUrl) {
        //NOTE: The two-argument constructor (HttpClient, ExecutorService) of SesameClientImpl does not seem to be
        //used and the executor is created by the initialize() method called from the default constructor. initialize()
        //sets the executor to Executors.newCachedThreadPool()
        //ExecutorService executor = Executors.newCachedThreadPool();

        return new DrafterSparqlSession(queryEndpointUrl, updateEndpointUrl, this.getHttpClient(), QUERY_EXECUTOR);
    }
}
