package drafter.rdf;

public interface Rewriter<T> {
    T rewrite(URIMapper mapper, T source);
}
