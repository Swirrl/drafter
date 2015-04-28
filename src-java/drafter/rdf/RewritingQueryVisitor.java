package drafter.rdf;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryVisitor;
import com.hp.hpl.jena.query.SortCondition;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.core.Prologue;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.core.VarExprList;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.Template;

import java.util.List;
import java.util.Map;

public class RewritingQueryVisitor implements QueryVisitor {
    private final URIMapper mapper;
    private Query result;

    public RewritingQueryVisitor(URIMapper mapper) { this.mapper = mapper; }

    @Override public void startVisit(Query query) {
        this.result = new Query();
    }

    @Override public void visitPrologue(Prologue prologue) {
        PrefixMapping newMapping = Rewriters.prefixMappingRewriter.rewrite(this.mapper, prologue.getPrefixMapping());
        this.result.setPrefixMapping(newMapping);
    }

    @Override public void visitResultForm(Query query) {
        //NOTE: Query visits the 'visit*ResultForm' methods based on the real query type
        //e.g. ASK, CONSTRUCT etc.
    }

    @Override public void visitSelectResultForm(Query query) {
        this.result.setDistinct(query.isDistinct());
        this.result.setQuerySelectType();
        this.result.setQueryResultStar(query.isQueryResultStar());

        this.visitQueryProjection(query.getProject());
    }

    @Override public void visitConstructResultForm(Query query) {
        Template newTemplate = Rewriters.templateRewriter.rewrite(this.mapper, query.getConstructTemplate());
        this.result.setQueryConstructType();
        this.result.setConstructTemplate(newTemplate);
    }

    @Override public void visitDescribeResultForm(Query query) {
        this.result.setQueryDescribeType();
        this.visitQueryProjection(query.getProject());
    }

    private void visitQueryProjection(VarExprList vel) {
        //VarExprList contains two collections - a List<Var> and a Map<Var, Expr>
        //Each Var in the var list represents a single variable e.g. ?s
        //Each pair in the Var -> Expr map represents an aliased variable e.g (COUNT(*) as ?c) will be an entry (Var(c), CountExpr)
        //WARNING: All the keys in the Map are included in the var list so don't add them twice!

        //vars don't need to be re-written?
        Map<Var, Expr> exprMap = vel.getExprs();

        //re-write all expression values
        for(Map.Entry<Var, Expr> kvp : exprMap.entrySet()) {
            Expr newExpr = Rewriters.exprRewriter.rewrite(this.mapper, kvp.getValue());
            this.result.addResultVar(kvp.getKey(), newExpr);
        }

        //add all plain vars not mapped to an expression
        for(Var plainVar : vel.getVars()) {
            if(! exprMap.containsKey(plainVar)) {
                this.result.addResultVar(plainVar);
            }
        }
    }

    @Override public void visitAskResultForm(Query query) {
        this.result.setQueryAskType();
    }

    @Override public void visitDatasetDecl(Query query) {
        if(query.hasDatasetDescription()) {
            for(String namedGraph : query.getNamedGraphURIs()) {
                this.result.addNamedGraphURI(this.mapper.mapURIString(namedGraph));
            }
            for(String defaultGraph : query.getGraphURIs()) {
                this.result.addGraphURI(this.mapper.mapURIString(defaultGraph));
            }
        }
    }

    @Override public void visitQueryPattern(Query query) {
        Element newQueryPattern = Rewriters.elementRewriter.rewrite(this.mapper, query.getQueryPattern());
        this.result.setQueryPattern(newQueryPattern);
    }

    @Override public void visitGroupBy(Query query) {
        VarExprList vel = query.getGroupBy();


        Map<Var, Expr> exprMap = vel.getExprs();

        //add all vars with corresponding expressions
        for(Map.Entry<Var, Expr> kvp : exprMap.entrySet()) {
            Var newVar = Rewriters.varRewriter.rewrite(mapper, kvp.getKey());
            Expr newExpr = Rewriters.exprRewriter.rewrite(mapper, kvp.getValue());

            this.result.addGroupBy(newVar, newExpr);
        }

        //add all plain vars not mapped to an expression
        for(Var v : vel.getVars()) {
            if(! exprMap.containsKey(v)) {
                Var newVar = Rewriters.varRewriter.rewrite(this.mapper, v);
                this.result.addGroupBy(newVar);
            }
        }
    }

    @Override public void visitHaving(Query query) {
        for(Expr e : query.getHavingExprs()) {
            Expr newExpr = Rewriters.exprRewriter.rewrite(this.mapper, e);
            this.result.addHavingCondition(newExpr);
        }
    }

    @Override public void visitOrderBy(Query query) {
        //WARNING: orderBy can return null!
        List<SortCondition> orderBy = query.getOrderBy();
        if(orderBy != null) {
            for(SortCondition cond : orderBy) {
                this.result.addOrderBy(Rewriters.sortConditionRewriter.rewrite(this.mapper, cond));
            }
        }
    }

    @Override public void visitLimit(Query query) {
        this.result.setLimit(query.getLimit());
    }

    @Override public void visitOffset(Query query) {
        this.result.setOffset(query.getOffset());
    }

    @Override public void visitValues(Query query) {
        if(query.hasValues()) {
            List<Binding> newBindings = Rewriters.rewriteList(Rewriters.bindingRewriter, this.mapper, query.getValuesData());
            List<Var> newVars = Rewriters.rewriteList(Rewriters.varRewriter, this.mapper, query.getValuesVariables());
            this.result.setValuesDataBlock(newVars, newBindings);
        }
    }

    @Override public void finishVisit(Query query) { }

    public Query getResult() {
        if(this.result == null) throw new IllegalStateException("No query visited");
        else return this.result;
    }
}
