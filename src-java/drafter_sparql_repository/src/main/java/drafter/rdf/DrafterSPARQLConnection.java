package drafter.rdf;

import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sparql.SPARQLConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.http.client.SparqlSession;

public class DrafterSPARQLConnection extends SPARQLConnection {

    public DrafterSPARQLConnection(SPARQLRepository repository, SparqlSession sparqlSession) {
        super(repository, sparqlSession);
    }

    @Override public BooleanQuery prepareBooleanQuery(QueryLanguage ql, String query, String base) throws RepositoryException, MalformedQueryException {
        return handleOp(super.prepareBooleanQuery(ql, query, base));
    }

    @Override public GraphQuery prepareGraphQuery(QueryLanguage ql, String query, String base) throws RepositoryException, MalformedQueryException {
        return handleOp(super.prepareGraphQuery(ql, query, base));
    }

    @Override public TupleQuery prepareTupleQuery(QueryLanguage ql, String query, String base) throws RepositoryException, MalformedQueryException {
        return handleOp(super.prepareTupleQuery(ql, query, base));
    }

    @Override public Update prepareUpdate(QueryLanguage ql, String update, String baseURI) throws RepositoryException, MalformedQueryException {
        return handleOp(super.prepareUpdate(ql, update, baseURI));
    }

    private static <O extends Operation> O handleOp(O op) {
        op.setIncludeInferred(false);
        return op;
    }
}
