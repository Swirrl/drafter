package drafter.rdf;

import com.hp.hpl.jena.sparql.syntax.*;

public class RewritingElementVisitor implements ElementVisitor {
    private Element element;
    private final URIMapper mapper;

    public RewritingElementVisitor(URIMapper mapper) { this.mapper = mapper; }

    @Override public void visit(ElementTriplesBlock elementTriplesBlock) {
        this.rewriteWith(elementTriplesBlock, Rewriters.elementTriplesBlockRewriter);
    }

    @Override public void visit(ElementPathBlock elementPathBlock) {
        this.rewriteWith(elementPathBlock, Rewriters.elementPathBlockRewriter);
    }

    @Override public void visit(ElementFilter elementFilter) {
        this.rewriteWith(elementFilter, Rewriters.elementFilterRewriter);
    }

    @Override public void visit(ElementAssign elementAssign) {
        this.rewriteWith(elementAssign, Rewriters.elementAssignRewriter);
    }

    @Override public void visit(ElementBind elementBind) {
        this.rewriteWith(elementBind, Rewriters.elementBindRewriter);
    }

    @Override public void visit(ElementData elementData) {
        this.rewriteWith(elementData, Rewriters.elementDataRewriter);
    }

    @Override public void visit(ElementUnion elementUnion) {
        this.rewriteWith(elementUnion, Rewriters.elementUnionRewriter);
    }

    @Override public void visit(ElementOptional elementOptional) {
        this.rewriteWith(elementOptional, Rewriters.elementOptionalRewriter);
    }

    @Override public void visit(ElementGroup elementGroup) {
        this.rewriteWith(elementGroup, Rewriters.elementGroupRewriter);
    }

    @Override public void visit(ElementDataset elementDataset) {
        this.rewriteWith(elementDataset, Rewriters.elementDatasetRewriter);
    }

    @Override public void visit(ElementNamedGraph elementNamedGraph) {
        this.rewriteWith(elementNamedGraph, Rewriters.elementNamedGraphRewriter);
    }

    @Override public void visit(ElementExists elementExists) {
        this.rewriteWith(elementExists, Rewriters.elementExistsRewriter);
    }

    @Override public void visit(ElementNotExists elementNotExists) {
        this.rewriteWith(elementNotExists, Rewriters.elementNotExistsRewriter);
    }

    @Override public void visit(ElementMinus elementMinus) {
        this.rewriteWith(elementMinus, Rewriters.elementMinusRewriter);
    }

    @Override public void visit(ElementService elementService) {
        //NOTE: Element service URIs should not be rewritten
        this.element = elementService;
    }

    @Override public void visit(ElementSubQuery elementSubQuery) {
        this.rewriteWith(elementSubQuery, Rewriters.elementSubQueryRewriter);
    }

    public Element getResult() {
        if(this.element == null) throw new IllegalStateException("No element visited");
        else return this.element;
    }

    private <E extends Element> void rewriteWith(E sourceElement, Rewriter<E> rewriter) {
        this.element = rewriter.rewrite(this.mapper, sourceElement);
    }
}
