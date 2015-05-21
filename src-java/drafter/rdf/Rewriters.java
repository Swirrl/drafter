package drafter.rdf;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.SortCondition;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.graph.NodeTransform;
import com.hp.hpl.jena.sparql.path.Path;
import com.hp.hpl.jena.sparql.syntax.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Rewriters {
    public static final Rewriter<Query> queryRewriter = new QueryRewriter();
    public static final Rewriter<Node> nodeRewriter = new NodeRewriter();
    public static final Rewriter<Expr> exprRewriter = new ExprRewriter();
    public static final Rewriter<Var> varRewriter = new VarRewriter();
    public static final Rewriter<Path> pathRewriter = new PathRewriter();

    public static final Rewriter<Element> elementRewriter = new ElementRewriter();
    public static final Rewriter<ElementUnion> elementUnionRewriter = new ElementUnionRewriter();
    public static final Rewriter<ElementSubQuery> elementSubQueryRewriter = new ElementSubQueryRewriter();
    public static final Rewriter<ElementPathBlock> elementPathBlockRewriter = new ElementPathBlockRewriter();
    public static final Rewriter<ElementTriplesBlock> elementTriplesBlockRewriter = new ElementTriplesBlockRewriter();
    public static final Rewriter<ElementFilter> elementFilterRewriter = new ElementFilterRewriter();
    public static final Rewriter<ElementAssign> elementAssignRewriter = new ElementAssignRewriter();
    public static final Rewriter<ElementBind> elementBindRewriter = new ElementBindRewriter();
    public static final Rewriter<ElementData> elementDataRewriter = new ElementDataRewriter();
    public static final Rewriter<ElementOptional> elementOptionalRewriter = new ElementOptionalRewriter();
    public static final Rewriter<ElementGroup> elementGroupRewriter = new ElementGroupRewriter();
    public static final Rewriter<ElementDataset> elementDatasetRewriter = new ElementDatasetRewriter();
    public static final Rewriter<ElementNamedGraph> elementNamedGraphRewriter = new ElementNamedGraphRewriter();
    public static final Rewriter<ElementExists> elementExistsRewriter = new ElementExistsRewriter();
    public static final Rewriter<ElementNotExists> elementNotExistsRewriter = new ElementNotExistsRewriter();
    public static final Rewriter<ElementMinus> elementMinusRewriter = new ElementMinusRewriter();

    public static final Rewriter<Triple> tripleRewriter = new TripleRewriter();
    public static final Rewriter<Binding> bindingRewriter = new BindingRewriter();
    public static final Rewriter<TriplePath> triplePathRewriter = new TriplePathRewriter();
    public static final Rewriter<BasicPattern> basicPatternRewriter = new BasicPatternRewriter();
    public static final Rewriter<Template> templateRewriter = new TemplateRewriter();
    public static final Rewriter<SortCondition> sortConditionRewriter = new SortConditionRewriter();
    public static final Rewriter<PrefixMapping> prefixMappingRewriter = new PrefixMappingRewriter();

    public static Query rewriteSPARQLQuery(URIMapper mapper, Query query) {
        RewritingQueryVisitor visitor = new RewritingQueryVisitor(mapper, true);
        query.visit(visitor);
        return visitor.getResult();
    }

    public static <T> List<T> rewriteList(Rewriter<T> rewriter, URIMapper mapper, List<T> sourceList) {
        List<T> destList = new ArrayList<T>();
        for(T item : sourceList) {
            destList.add(rewriter.rewrite(mapper, item));
        }
        return destList;
    }

    private static class RewritingNodeTransform implements NodeTransform {
        private final URIMapper mapper;

        public RewritingNodeTransform(URIMapper mapper) { this.mapper = mapper; }

        @Override public Node convert(Node node) {
            RewritingNodeVisitor visitor = new RewritingNodeVisitor(this.mapper);
            return (Node)node.visitWith(visitor);
        }
    }

    private static class QueryRewriter implements Rewriter<Query> {
        @Override public Query rewrite(URIMapper mapper, Query source) {
            RewritingQueryVisitor visitor = new RewritingQueryVisitor(mapper);
            source.visit(visitor);
            return visitor.getResult();
        }
    }

    private static class ExprRewriter implements Rewriter<Expr> {
        @Override public Expr rewrite(URIMapper mapper, Expr source) {
            return source.applyNodeTransform(new RewritingNodeTransform(mapper));
        }
    }

    private static class NodeRewriter implements Rewriter<Node> {
        @Override public Node rewrite(URIMapper mapper, Node source) {
            RewritingNodeVisitor visitor = new RewritingNodeVisitor(mapper);
            return (Node)source.visitWith(visitor);
        }
    }

    private static class VarRewriter implements Rewriter<Var> {
        @Override public Var rewrite(URIMapper mapper, Var source) {
            //TODO: Should vars be re-written?
            return (Var)nodeRewriter.rewrite(mapper, source);
        }
    }

    private static class ElementRewriter implements Rewriter<Element> {
        @Override public Element rewrite(URIMapper mapper, Element source) {
            RewritingElementVisitor visitor = new RewritingElementVisitor(mapper);
            source.visit(visitor);
            return visitor.getResult();
        }
    }

    private static class BindingRewriter implements Rewriter<Binding> {
        @Override public Binding rewrite(URIMapper mapper, Binding source) {
            BindingMap bindingMap = BindingFactory.create();
            for(Iterator<Var> it = source.vars(); it.hasNext(); ) {
                Var currentVar = it.next();
                Node mappedNode = nodeRewriter.rewrite(mapper, source.get(currentVar));
                bindingMap.add(currentVar, mappedNode);
            }
            return bindingMap;
        }
    }

    private static class ElementSubQueryRewriter implements Rewriter<ElementSubQuery> {
        @Override public ElementSubQuery rewrite(URIMapper mapper, ElementSubQuery source) {
            Query newQuery = queryRewriter.rewrite(mapper, source.getQuery());
            return new ElementSubQuery(newQuery);
        }
    }

    private static class ElementUnionRewriter implements Rewriter<ElementUnion> {
        @Override public ElementUnion rewrite(URIMapper mapper, ElementUnion source) {
            ElementUnion newUnion = new ElementUnion();
            for(Element e : source.getElements()) {
                newUnion.addElement(elementRewriter.rewrite(mapper, e));
            }
            return newUnion;
        }
    }

    private static class ElementDataRewriter implements Rewriter<ElementData> {
        @Override public ElementData rewrite(URIMapper mapper, ElementData source) {
            ElementData newData = new ElementData();

            //vars should not be re-written?
            for(Var var : source.getVars()) {
                newData.add(var);
            }

            //rewrite bindings
            for(Binding binding : source.getRows()) {
                Binding newBinding = bindingRewriter.rewrite(mapper, binding);
                newData.add(newBinding);
            }

            return newData;
        }
    }

    private static class ElementBindRewriter implements Rewriter<ElementBind> {
        @Override public ElementBind rewrite(URIMapper mapper, ElementBind source) {
            Var newVar = varRewriter.rewrite(mapper, source.getVar());
            Expr newExpr = exprRewriter.rewrite(mapper, source.getExpr());
            return new ElementBind(newVar, newExpr);
        }
    }

    private static class ElementDatasetRewriter implements Rewriter<ElementDataset> {
        @Override public ElementDataset rewrite(URIMapper mapper, ElementDataset source) {
            //NOTE: The DatasetGraph associated with source appears to represent the data store itself
            //and is not a syntactic element so does not need to be re-written
            Element newPatternElement = elementRewriter.rewrite(mapper, source.getPatternElement());
            return new ElementDataset(source.getDataset(), newPatternElement);
        }
    }

    private static class ElementGroupRewriter implements Rewriter<ElementGroup> {
        @Override public ElementGroup rewrite(URIMapper mapper, ElementGroup source) {
            ElementGroup newGroup = new ElementGroup();
            for(Element e : source.getElements()) {
                newGroup.addElement(elementRewriter.rewrite(mapper, e));
            }
            return newGroup;
        }
    }

    private static class ElementOptionalRewriter implements Rewriter<ElementOptional> {
        @Override public ElementOptional rewrite(URIMapper mapper, ElementOptional source) {
            Element newOptional = elementRewriter.rewrite(mapper, source.getOptionalElement());
            return new ElementOptional(newOptional);
        }
    }

    private static class ElementExistsRewriter implements Rewriter<ElementExists> {
        @Override public ElementExists rewrite(URIMapper mapper, ElementExists source) {
            Element newElement = elementRewriter.rewrite(mapper, source.getElement());
            return new ElementExists(newElement);
        }
    }

    private static class ElementNotExistsRewriter implements Rewriter<ElementNotExists> {
        @Override public ElementNotExists rewrite(URIMapper mapper, ElementNotExists source) {
            Element newElement = elementRewriter.rewrite(mapper, source.getElement());
            return new ElementNotExists(newElement);
        }
    }

    private static class ElementNamedGraphRewriter implements Rewriter<ElementNamedGraph> {
        @Override public ElementNamedGraph rewrite(URIMapper mapper, ElementNamedGraph source) {
            Node newNameNode = nodeRewriter.rewrite(mapper, source.getGraphNameNode());
            Element newElement = elementRewriter.rewrite(mapper, source.getElement());
            return new ElementNamedGraph(newNameNode, newElement);
        }
    }

    private static class ElementMinusRewriter implements Rewriter<ElementMinus> {
        @Override public ElementMinus rewrite(URIMapper mapper, ElementMinus source) {
            Element newMinusElement = elementRewriter.rewrite(mapper, source.getMinusElement());
            return new ElementMinus(newMinusElement);
        }
    }

    private static class PathRewriter implements Rewriter<Path> {
        @Override public Path rewrite(URIMapper mapper, Path path) {
            RewritingPathVisitor visitor = new RewritingPathVisitor(mapper);
            path.visit(visitor);
            return visitor.getResult();
        }
    }

    private static class TripleRewriter implements Rewriter<Triple> {
        @Override public Triple rewrite(URIMapper mapper, Triple triple) {
            Node newSubject = nodeRewriter.rewrite(mapper, triple.getSubject());
            Node newPredicate = nodeRewriter.rewrite(mapper, triple.getPredicate());
            Node newObject = nodeRewriter.rewrite(mapper, triple.getObject());
            return new Triple(newSubject, newPredicate, newObject);
        }
    }

    private static class TriplePathRewriter implements Rewriter<TriplePath> {
        @Override public TriplePath rewrite(URIMapper mapper, TriplePath triplePath) {
            if(triplePath.isTriple()) {
                Triple newTriple = tripleRewriter.rewrite(mapper, triplePath.asTriple());
                return new TriplePath(newTriple);
            }
            else {
                Node newSubject = nodeRewriter.rewrite(mapper, triplePath.getSubject());
                Path newPath = pathRewriter.rewrite(mapper, triplePath.getPath());
                Node newObject = nodeRewriter.rewrite(mapper, triplePath.getObject());
                return new TriplePath(newSubject, newPath, newObject);
            }
        }
    }

    private static class ElementTriplesBlockRewriter implements Rewriter<ElementTriplesBlock> {
        @Override public ElementTriplesBlock rewrite(URIMapper mapper, ElementTriplesBlock triplesBlock) {
            ElementTriplesBlock newTripleBlock = new ElementTriplesBlock();
            for(Triple t: triplesBlock.getPattern()) {
                Triple newTriple = tripleRewriter.rewrite(mapper, t);
                newTripleBlock.addTriple(newTriple);
            }
            return newTripleBlock;
        }
    }

    private static class ElementPathBlockRewriter implements Rewriter<ElementPathBlock> {
        @Override public ElementPathBlock rewrite(URIMapper mapper, ElementPathBlock source) {
            ElementPathBlock newPathBlock = new ElementPathBlock();
            for(TriplePath tp : source.getPattern()) {
                newPathBlock.addTriplePath(triplePathRewriter.rewrite(mapper, tp));
            }
            return newPathBlock;
        }
    }

    private static class ElementFilterRewriter implements Rewriter<ElementFilter> {
        @Override public ElementFilter rewrite(URIMapper mapper, ElementFilter source) {
            Expr newExpr = exprRewriter.rewrite(mapper, source.getExpr());
            return new ElementFilter(newExpr);
        }
    }

    private static class ElementAssignRewriter implements Rewriter<ElementAssign> {
        @Override public ElementAssign rewrite(URIMapper mapper, ElementAssign source) {
            Var newVar = varRewriter.rewrite(mapper, source.getVar());
            Expr newExpr = exprRewriter.rewrite(mapper, source.getExpr());
            return new ElementAssign(newVar, newExpr);
        }
    }

    private static class BasicPatternRewriter implements Rewriter<BasicPattern> {
        @Override public BasicPattern rewrite(URIMapper mapper, BasicPattern source) {
            List<Triple> newTriples = new ArrayList<>();
            for(Triple t : source) {
                newTriples.add(tripleRewriter.rewrite(mapper, t));
            }
            return BasicPattern.wrap(newTriples);
        }
    }

    private static class TemplateRewriter implements Rewriter<Template> {
        @Override public Template rewrite(URIMapper mapper, Template source) {
            BasicPattern newBp = basicPatternRewriter.rewrite(mapper, source.getBGP());
            return new Template(newBp);
        }
    }

    private static class SortConditionRewriter implements Rewriter<SortCondition> {
        @Override public SortCondition rewrite(URIMapper mapper, SortCondition source) {
            Expr newExpr = exprRewriter.rewrite(mapper, source.getExpression());
            return new SortCondition(newExpr, source.getDirection());
        }
    }

    private static class PrefixMappingRewriter implements Rewriter<PrefixMapping> {
        @Override public PrefixMapping rewrite(URIMapper mapper, PrefixMapping source) {
            //WARNING: Only works if mapped URI exactly matches the live graph URI!
            //e.g. with the live->draft mapping {http://live -> http://draft}
            //pf:http://live is correctly re-written to pf:http://draft
            //but
            //pf:http://live/something will not be re-written!
            //TODO: parse URI and check prefix?
            PrefixMapping newPrefixMapping = PrefixMapping.Factory.create();

            for(Map.Entry<String, String> mapping : source.getNsPrefixMap().entrySet()) {
                newPrefixMapping.setNsPrefix(mapping.getKey(), mapper.mapURIString(mapping.getValue()));
            }

            return newPrefixMapping;
        }
    }
}
