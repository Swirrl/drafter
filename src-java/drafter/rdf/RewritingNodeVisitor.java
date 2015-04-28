package drafter.rdf;

import com.hp.hpl.jena.graph.*;
import com.hp.hpl.jena.graph.impl.LiteralLabel;
import com.hp.hpl.jena.rdf.model.AnonId;

public class RewritingNodeVisitor implements NodeVisitor {
    private final URIMapper mapper;

    public RewritingNodeVisitor(URIMapper mapper) { this.mapper = mapper; }

    @Override public Object visitAny(Node_ANY node_any) {
        return node_any;
    }

    @Override public Object visitBlank(Node_Blank node_blank, AnonId anonId) {
        return node_blank;
    }

    @Override public Object visitLiteral(Node_Literal node_literal, LiteralLabel literalLabel) {
        Object value = literalLabel.getValue();
        if(value instanceof String) {
            String newLit = this.mapper.mapURIString((String)value);
            return NodeFactory.createLiteral(newLit);
        }
        else { return node_literal; }
    }

    @Override public Object visitURI(Node_URI node_uri, String s) {
        String mappedUri = this.mapper.mapURIString(s);
        return NodeFactory.createURI(mappedUri);
    }

    @Override public Object visitVariable(Node_Variable node_variable, String s) {
        return node_variable;
    }
}
