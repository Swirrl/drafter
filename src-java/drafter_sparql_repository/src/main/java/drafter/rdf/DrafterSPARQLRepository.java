package drafter.rdf;

import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;

public class DrafterSPARQLRepository extends SPARQLRepository {

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
}
