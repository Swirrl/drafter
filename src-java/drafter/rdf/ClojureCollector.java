package drafter.rdf;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

/**
 * A helper class to help Clojure walk the SPARQL AST and accumulate
 * the tree into a sequence which can be filtered and manipulated from
 * within Clojure.
 *
 * WARNING: this tree containts mutable references to the AST, so
 * "changes" here are real changes, i.e. mutations.  You have been
 * warned!
 */
public class ClojureCollector extends QueryModelVisitorBase<RuntimeException> {

    private final List<QueryModelNode> qmnPatterns = new ArrayList<QueryModelNode>();

    public static List<QueryModelNode> process(QueryModelNode node) {
        ClojureCollector collector = new ClojureCollector();
        node.visit(collector); // visit the root
        node.visitChildren(collector);
        return collector.getNodeCollector();
    }

    public List<QueryModelNode> getNodeCollector() {
        return qmnPatterns;
    }

    public void meet(Add node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(And node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(ArbitraryLengthPath node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Avg node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(BindingSetAssignment node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(BNodeGenerator node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Bound node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Clear node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Coalesce node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Compare node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(CompareAll node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(CompareAny node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(DescribeOperator node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Copy node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Count node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Create node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Datatype node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(DeleteData node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Difference node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Distinct node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(EmptySet node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Exists node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Extension node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(ExtensionElem node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Filter node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(FunctionCall node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Group node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(GroupConcat node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(GroupElem node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(If node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(In node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(InsertData node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Intersection node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(IRIFunction node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(IsBNode node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(IsLiteral node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(IsNumeric node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(IsResource node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(IsURI node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Join node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Label node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Lang node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(LangMatches node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(LeftJoin node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Like node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Load node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(LocalName node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(MathExpr node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Max node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Min node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Modify node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Move node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(MultiProjection node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Namespace node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Not node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Or node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Order node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(OrderElem node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Projection node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(ProjectionElem node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(ProjectionElemList node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(QueryRoot node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Reduced node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Regex node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(SameTerm node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Sample node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Service node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(SingletonSet node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Slice node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(StatementPattern node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Str node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Sum node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Union node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(ValueConstant node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(ListMemberOperator node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(Var node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

    public void meet(ZeroLengthPath node)
    {
        qmnPatterns.add(node);
        node.visitChildren(this);
    }

}
