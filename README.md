# Aggregation Tool 1.4.1   [![Build Status](https://travis-ci.org/meox/aggregate.svg?branch=master)](https://travis-ci.org/meox/aggregate)
fast CSV aggragation tool for Unix

## Command line options
```
aggregate [options]
-k               are the keys-elements used for aggregation
-s               are the sums-elements used for aggregation
-p               are the sums-elements used for projection
-r               specify a register ex.: -r %t:123; you can use that register inside a projection list
--skip-line      number of rows (starting from head) to skip
-f               is the file to load (coudl be used serveral times)
--path           is the path where to find csv input files
--input-sep      is the csv input separator
--output-sep     is the csv output separator
--output-file    is the output file"
--no-value       specify witch is the "no value" (default: -1)
--set-header     specify the header to use for the output csv
--dry-run        execute some test on input parameter
--help           print this help and exit
--version        print the version number and exit
```

Example:

./aggregate -r %t:1982 -k "2-20" -s "21-35" -p "%t;1-35" --skip-line 1 --path /home/meox/rawcsv --reuse-skipped --output-file out.csv

