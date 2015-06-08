package drafter.rdf;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.path.*;

public class RewritingPathVisitor implements PathVisitor{
    private interface Path0Constructor<P extends P_Path0> { P createFrom(Node node); }
    private interface Path1Constructor<P extends P_Path1> { P createFrom(Path path); }
    private interface Path2Constructor<P extends P_Path2> { P createFrom(Path left, Path right); }

    private Path result;
    private final URIMapper mapper;

    public RewritingPathVisitor(URIMapper mapper) { this.mapper = mapper; }

    @Override public void visit(P_Link p_link) {
        this.rewritePath0(p_link, new Path0Constructor<P_Link>() {
            @Override public P_Link createFrom(Node node) {
                return new P_Link(node);
            }
        });
    }

    @Override public void visit(final P_ReverseLink p_reverseLink) {
        this.rewritePath0(p_reverseLink, new Path0Constructor<P_ReverseLink>() {
            @Override public P_ReverseLink createFrom(Node node) {
                return new P_ReverseLink(node);
            }
        });
    }

    @Override public void visit(P_NegPropSet p_negPropSet) {
        P_NegPropSet newNegPropSet = new P_NegPropSet();
        for(P_Path0 p : p_negPropSet.getNodes()) {
            P_Path0 newPathNode = (P_Path0)Rewriters.pathRewriter.rewrite(this.mapper, p);
            newNegPropSet.add(newPathNode);
        }
        this.result = newNegPropSet;
    }

    @Override public void visit(P_Inverse p_inverse) {
        this.rewritePath1(p_inverse, new Path1Constructor<P_Inverse>() {
            @Override public P_Inverse createFrom(Path path) {
                return new P_Inverse(path);
            }
        });
    }

    @Override public void visit(final P_Mod p_mod) {
        this.rewritePath1(p_mod, new Path1Constructor<P_Mod>() {
            @Override public P_Mod createFrom(Path path) {
                return new P_Mod(path, p_mod.getMin(), p_mod.getMax());
            }
        });
    }

    @Override public void visit(final P_FixedLength p_fixedLength) {
        this.rewritePath1(p_fixedLength, new Path1Constructor<P_FixedLength>() {
            @Override public P_FixedLength createFrom(Path path) {
                return new P_FixedLength(path, p_fixedLength.getCount());
            }
        });
    }

    @Override public void visit(P_Distinct p_distinct) {
        this.rewritePath1(p_distinct, new Path1Constructor<P_Distinct>() {
            @Override public P_Distinct createFrom(Path path) {
                return new P_Distinct(path);
            }
        });
    }

    @Override public void visit(P_Multi p_multi) {
        this.rewritePath1(p_multi, new Path1Constructor<P_Multi>() {
            @Override public P_Multi createFrom(Path path) {
                return new P_Multi(path);
            }
        });
    }

    @Override public void visit(P_Shortest p_shortest) {
        this.rewritePath1(p_shortest, new Path1Constructor<P_Shortest>() {
            @Override public P_Shortest createFrom(Path path) {
                return new P_Shortest(path);
            }
        });
    }

    @Override public void visit(P_ZeroOrOne p_zeroOrOne) {
        this.rewritePath1(p_zeroOrOne, new Path1Constructor<P_ZeroOrOne>() {
            @Override public P_ZeroOrOne createFrom(Path path) {
                return new P_ZeroOrOne(path);
            }
        });
    }

    @Override public void visit(P_ZeroOrMore1 p_zeroOrMore1) {
        this.rewritePath1(p_zeroOrMore1, new Path1Constructor<P_ZeroOrMore1>() {
            @Override public P_ZeroOrMore1 createFrom(Path path) {
                return new P_ZeroOrMore1(path);
            }
        });
    }

    @Override public void visit(P_ZeroOrMoreN p_zeroOrMoreN) {
        this.rewritePath1(p_zeroOrMoreN, new Path1Constructor<P_ZeroOrMoreN>() {
            @Override public P_ZeroOrMoreN createFrom(Path path) {
                return new P_ZeroOrMoreN(path);
            }
        });
    }

    @Override public void visit(P_OneOrMore1 p_oneOrMore1) {
        this.rewritePath1(p_oneOrMore1, new Path1Constructor<P_OneOrMore1>() {
            @Override public P_OneOrMore1 createFrom(Path path) {
                return new P_OneOrMore1(path);
            }
        });
    }

    @Override public void visit(P_OneOrMoreN p_oneOrMoreN) {
        this.rewritePath1(p_oneOrMoreN, new Path1Constructor<P_OneOrMoreN>() {
            @Override public P_OneOrMoreN createFrom(Path path) {
                return new P_OneOrMoreN(path);
            }
        });
    }

    @Override public void visit(P_Alt p_alt) {
        this.rewritePath2(p_alt, new Path2Constructor<P_Alt>() {
            @Override public P_Alt createFrom(Path left, Path right) {
                return new P_Alt(left, right);
            }
        });
    }

    @Override public void visit(P_Seq p_seq) {
        this.rewritePath2(p_seq, new Path2Constructor<P_Seq>() {
            @Override public P_Seq createFrom(Path left, Path right) {
                return new P_Seq(left, right);
            }
        });
    }

    public Path getResult() {
        if(this.result == null) throw new IllegalStateException("No source path visited");
        else return this.result;
    }

    private <P extends P_Path0> void rewritePath0(P src, Path0Constructor<P> constructor) {
        Node newNode = Rewriters.nodeRewriter.rewrite(this.mapper, src.getNode());
        this.result = constructor.createFrom(newNode);
    }

    private <P extends P_Path1> void rewritePath1(P src, Path1Constructor<P> constructor) {
        Path newSubpath = Rewriters.pathRewriter.rewrite(this.mapper, src.getSubPath());
        this.result = constructor.createFrom(newSubpath);
    }

    private <P extends P_Path2> void rewritePath2(P src, Path2Constructor<P> constructor) {
        Path newLeft = Rewriters.pathRewriter.rewrite(this.mapper, src.getLeft());
        Path newRight = Rewriters.pathRewriter.rewrite(this.mapper, src.getRight());
        this.result = constructor.createFrom(newLeft, newRight);
    }
}
