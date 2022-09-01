# data-gen

Data generator for Drafter benchmarks. The drafter benchmarks rely on test data files of different sizes, split between 
different numbers of graphs and with different percentages of 'graph-referencing' statements. This project presents
a command-line interface to generating random data files with the required characteristics.

## Generating a single file

The `generate` task generates a single dataset of random data

    clj -M -m data-gen.main generate --statements 1000 --graphs 2 --output-file data.nq
    
This will write an output file of 1000 quads split between 2 graphs to `data.nq`. By default, none of the generates
statements reference the graphs in any of the subject, predicate or object positions. The total number of such statements
can be specified with the `--referential` option:

    clj -M -m data-gen.main generate --statements 1000 --graphs 2 --referential 5 -o data.nq
    
This results in 5 of the 1000 generated statements referencing the graphs. The percentage of graph-referencing statements
can be specified instead of the absolute number:

    clj -M -m data-gen.main generate --statements 1000 --g 2 --referential 2% -o data.nq
    
will generate 2% of the 1000 statements to reference the output graphs.

## Generating all test files

The benchmarks rely on a fixed set of data files which follow a naming convention which indicate the parameters used
to generate them. The generated file names all have the following format:

    data_[statements]k_[graphs]g_[graph-referencing]pc.nq
    
This indicates the parameters used for the number of statements, graphs and graph-referencing statements. For example,
the output file with the name `data_100k_10g_1pc.nq` contains:

* 100,000 statements
* within 10 graphs
* where 1% of statements reference a graph in the subject, predicate or object position

To generate all benchmark data: 

    clj -M -m data-gen.main generate-all --output-dir data
    
This will create the `data` directory if necessary and write all required data files into it.
    
