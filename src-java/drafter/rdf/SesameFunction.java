package drafter.rdf;

import org.openrdf.query.algebra.evaluation.function.Function;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.java.api.Clojure;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

public class SesameFunction implements Function {

    private final String uri;
    private final IFn f;
    private final IFn seq;

    public SesameFunction(String uri, IFn f) {
        this.uri = uri;
        this.f = f;
        seq = Clojure.var("clojure.core", "seq");
    }

    public Value evaluate(ValueFactory vFactory, Value... args) {
        ISeq argSeq = (ISeq) seq.invoke(args);
        return (Value) f.applyTo(argSeq);
    }

    public String getURI() {
        return this.uri;
    }

    public boolean equals(final Object other) {
        if(!(other instanceof SesameFunction)) {
            return false;
        }
        final SesameFunction sf = (SesameFunction) other;

        return this.getURI().equals(sf.getURI());
    }

    public int hashCode() {
        return this.getURI().hashCode();
    }
}
