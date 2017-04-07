package drafter.rdf;

import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.openrdf.OpenRDFException;
import org.openrdf.http.client.SparqlSession;
import org.openrdf.http.protocol.UnauthorizedException;
import org.openrdf.http.protocol.error.ErrorInfo;
import org.openrdf.http.protocol.error.ErrorType;
import org.openrdf.query.*;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.UnsupportedRDFormatException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class DrafterSparqlSession extends SparqlSession {
    /**
     * The longest URL length accepted by stardog. SPARQL queries which result in a URL longer than this length
     * should be sent as POST requests instead.
     */
    public static final int STARDOG_MAXIMUM_URL_LENGTH = 4083;

    public DrafterSparqlSession(String queryEndpointUrl, String updateEndpointUrl, HttpClient client, ExecutorService executor) {
        super(client, executor);
        this.setQueryURL(queryEndpointUrl);
        this.setUpdateURL(updateEndpointUrl);
    }

    @SuppressWarnings("unchecked")
    private static <T> T readField(Class cls, Object receiver, String fieldName) {
        try {
            Field f = cls.getDeclaredField(fieldName);
            f.setAccessible(true);
            return (T)f.get(receiver);
        } catch(NoSuchFieldException ex) {
            throw new RuntimeException(String.format("Field %s in class %s does not exist", fieldName, cls.getName()));
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(String.format("Field %s in class %s is not accessible", fieldName, cls.getName()));
        }
    }

    @SuppressWarnings("deprecation")
    /**
     * Constructs the parameters to be used by the HTTP request. This is based on the params member of SparqlSession
     * which is configured within the constructor and by setConnectionTimeout in the base class.
     */
    private HttpParams getHttpParams() {
        BasicHttpParams params = new BasicHttpParams();
        params.setBooleanParameter(ClientPNames.HANDLE_REDIRECTS, true);
        params.setParameter(ClientPNames.COOKIE_POLICY, CookiePolicy.RFC_2109);

        //set timeouts:
        // - SO_TIMEOUT is the timeout between consecutive data packets received by the underlying connection
        // - CONNECTION_TIMEOUT is the time to establish the TCP connection
        // - CONN_MANAGER_TIMEOUT is the timeout for obtaining a connection from the connection pool
        int socketTimeout = (int)this.getConnectionTimeout();
        params.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, socketTimeout);
        HttpConnectionParams.setConnectionTimeout(params, 100);
        params.setLongParameter(ClientPNames.CONN_MANAGER_TIMEOUT,1);

        return params;
    }

    private HttpClientContext getHttpContext() {
        return readField(SparqlSession.class, this, "httpContext");
    }

    /**
     * Inspects a HTTP response returned from the server and checks if it looks like a timeout.
     * @param response The HTTP response returned from the server
     * @return Whether the received response looks like a query timeout response
     */
    private boolean isStardogTimeoutResponse(HttpResponse response) {
        int statusCode = response.getStatusLine().getStatusCode();
        Header errorCodeHeader = response.getFirstHeader("SD-Error-Code");
        if (    statusCode == HttpURLConnection.HTTP_INTERNAL_ERROR &&
                errorCodeHeader != null &&
                "QueryEval".equals(errorCodeHeader.getValue())) {
            HttpEntity entity = response.getEntity();
            try {
                //inspect the message in the response to see if it indicates a query timeout
                String body = EntityUtils.toString(entity);
                return body != null && body.contains("exceeded query timeout");
            }
            catch (IOException ex) {
                return false;
            }
            catch (ParseException ex) {
                return false;
            }
        }
        else return false;
    }

    private static final String TIMEOUT_QUERY_PARAM_NAME = "timeout";

    private static void removeTimeoutQueryParams(List<NameValuePair> queryPairs) {
        List<NameValuePair> toRemove = new ArrayList<NameValuePair>();
        for(NameValuePair pair : queryPairs) {
            if(TIMEOUT_QUERY_PARAM_NAME.equals(pair.getName())) {
                toRemove.add(pair);
            }
        }
        queryPairs.removeAll(toRemove);
    }

    @Override protected List<NameValuePair> getQueryMethodParameters(QueryLanguage ql, String query, String baseURI, Dataset dataset, boolean includeInferred, int maxQueryTime, Binding... bindings) {
        List<NameValuePair> pairs = super.getQueryMethodParameters(ql, query, baseURI, dataset, includeInferred, maxQueryTime, bindings);

        //sesame adds a timeout=period_seconds query parameter if the maximum query time is set
        //remove this parameter and replace it with our own
        removeTimeoutQueryParams(pairs);

        //add timeout if specified (i.e. maxQueryTime > 0)
        if(maxQueryTime > 0) {
            //add Stardog timeout=period_ms query parameter
            //maxQueryTime is the maximum time in seconds whereas Stardog's timeout is measured in Milliseconds
            Integer timeoutMs = 1000 * maxQueryTime;
            pairs.add(new BasicNameValuePair(TIMEOUT_QUERY_PARAM_NAME, timeoutMs.toString()));
        }

        return pairs;
    }

    /**
     * Whether the given SPARQL query should be sent as the body of a POST request.
     * @param query The SPARQL query
     * @return Whether to use a POST request to submit the query
     */
    @Override protected boolean shouldUsePost(String query) {
        return query.length() > STARDOG_MAXIMUM_URL_LENGTH;
    }

    @SuppressWarnings("deprecation")
    @Override protected HttpResponse execute(HttpUriRequest method) throws IOException, OpenRDFException {
        //NOTE: the implementation of this method is based on SparqlSession.execute(HttpUriRequest)
        //This class cannot access the private HttpClientContext fields used by the base implementation
        //so fetches it using reflection(!). It also inspects the received response to check if it appears to indicate
        //a query timeout and throws a QueryInterruptedException in that case.
        HttpClient httpClient = getHttpClient();
        HttpClientContext httpContext = getHttpContext();

        boolean consume = true;

        HttpParams params = getHttpParams();
        method.setParams(params);
        HttpResponse response;
        try {
            response = httpClient.execute(method, httpContext);
        }
        catch (ConnectionPoolTimeoutException ex) {
            throw new QueryInterruptedException();
        }

        try {
            int httpCode = response.getStatusLine().getStatusCode();
            if (httpCode >= 200 && httpCode < 300 || httpCode == HttpURLConnection.HTTP_NOT_FOUND) {
                consume = false;
                return response; // everything OK, control flow can continue
            }
            else if (isStardogTimeoutResponse(response)) {
                throw new QueryInterruptedException();
            }
            else {
                switch (httpCode) {
                    case HttpURLConnection.HTTP_UNAUTHORIZED: // 401
                        throw new UnauthorizedException();
                    case HttpURLConnection.HTTP_UNAVAILABLE: // 503
                        throw new QueryInterruptedException();
                    default:
                        ErrorInfo errInfo = getErrorInfo(response);
                        // Throw appropriate exception
                        if (errInfo.getErrorType() == ErrorType.MALFORMED_DATA) {
                            throw new RDFParseException(errInfo.getErrorMessage());
                        }
                        else if (errInfo.getErrorType() == ErrorType.UNSUPPORTED_FILE_FORMAT) {
                            throw new UnsupportedRDFormatException(errInfo.getErrorMessage());
                        }
                        else if (errInfo.getErrorType() == ErrorType.MALFORMED_QUERY) {
                            throw new MalformedQueryException(errInfo.getErrorMessage());
                        }
                        else if (errInfo.getErrorType() == ErrorType.UNSUPPORTED_QUERY_LANGUAGE) {
                            throw new UnsupportedQueryLanguageException(errInfo.getErrorMessage());
                        }
                        else {
                            throw new RepositoryException(errInfo.toString());
                        }
                }
            }
        }
        finally {
            if (consume) {
                EntityUtils.consumeQuietly(response.getEntity());
            }
        }
    }
}