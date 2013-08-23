CSVJoin
=======

Simple utility for joining CSV files

## Usage
```
$ python csvjoin/csvjoin.py 
usage: csvjoin.py [-h] [--firstprefix FIRSTPREFIX]
                  [--secondprefix SECONDPREFIX]
                  firstfile secondfile firstkey secondkey outputfile
```

## Requirements
* [Redis](http://redis.io) is required to store the contents of the first part of the join. Installation instructions [here](http://redis.io/download).

