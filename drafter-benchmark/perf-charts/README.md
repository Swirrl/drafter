# perf-charts

Charts for drafter benchmark results. Once benchmark results have been collected, they can be compared by visualising
how performance is affected by the data input parameters (number of statements, number of graphs, percentage of graph-referencing
statements). Comparisons for each of these dimensions can be made by fixing the other dimensions and rendering a chart
of the remaining dimension on the total time taken. All such charts are generated at once from a collection of result
CSV files.

## Usage

Write all performance charts to an output directory from a collection of result files with:

    clj -M -m perf-charts.main --directory output-directory jmh-result-version1.csv jmh-result-version2.csv ...
    
The output directory will be created if necessary and all graphs will be written directly underneath. The input CSV
file names should all follow the format `jmh-result-[version].csv`. The `version` component is arbitrary and will be
used to identify the source result set in the each chart.

Each of the charts are written as PNG image files with a file name which indicates the fixed dimensions. File names
all have the format:

    [benchmark_name]-([statements]k)?-([graphs]g)?-([ref-statements])?.nq 
    
Each output file will the `benchmark_name` prefix and two of the `statements`, `graphs` and `ref-statements` components.
These indicate the fixed dimensions (and therefore the dimension which varies). For example the file `append-10g-1pc.nq`
fixes the number of graphs and the percentage of graph-referencing statements to 10 and 1% respectively. The chart
therefore shows how performance is affected as the number of statements increases. The file `delete-100k-0pc.nq` shows
how performance is affected for deleting 100k non-graph-referencing statements as the number of graphs increases.
