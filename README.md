# Aggregation Tool 1.1
fast C++ tool to aggragate csv

###command line options
```
aggregate [options]
 -k               are the keys-elements used for aggregation
 -s               are the sums-elements used for aggregation
 -p               are the sums-elements used for projection
 -r               specify a register ex.: -r %t:123; you can use that register inside a projection list
 --skip-line      number of rows (starting from head) to skip
 -f               is the file to load (coudl be used serveral times)
 --path           is the path where to find csv input files
 --reuse-skipped  if present the skipped line will be reinserted into the output file
 --input-sep      is the csv input separator
 --output-sep     is the csv output separator
 --output-file    is the output file
 --no-value       specify witch is the "no value" (default: "-1")
 --dry-run        execute some test on input parameter
```
