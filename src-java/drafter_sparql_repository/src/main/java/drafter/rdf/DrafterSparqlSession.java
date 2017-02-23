package drafter.rdf;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.openrdf.OpenRDFException;
import org.openrdf.http.client.SparqlSession;
import org.openrdf.http.protocol.UnauthorizedException;
import org.openrdf.http.protocol.error.ErrorInfo;
import org.openrdf.http.protocol.error.ErrorType;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryInterruptedException;
import org.openrdf.query.UnsupportedQueryLanguageException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.UnsupportedRDFormatException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.util.concurrent.ExecutorService;

public class DrafterSparqlSession extends SparqlSession {
    public DrafterSparqlSession(String queryEndpointUrl, String updateEndpointUrl, HttpClient client, ExecutorService executor) {
        super(client, executor);
        this.setQueryURL(queryEndpointUrl);
        this.setUpdateURL(updateEndpointUrl);
    }

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

    private HttpParams getHttpParams() {
        //oh jesus...
        return readField(SparqlSession.class, this, "params");
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

    @Override protected HttpResponse execute(HttpUriRequest method) throws IOException, OpenRDFException {
        HttpParams params = getHttpParams();
        HttpClient httpClient = getHttpClient();
        HttpClientContext httpContext = getHttpContext();

        boolean consume = true;
        method.setParams(params);
        HttpResponse response = httpClient.execute(method, httpContext);

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
