# RDF-TDAA

RDF-TDAA: Efficient **RDF** Indexing with a **T**rie Based on **D**irectly **A**ddressable **A**rrays

## How to build

1. Clone this project

```shell
git clone git@github.com:MKMaS-GUET/RDF-TDAA.git
git submodule update --init
```

Maybe it's need to update the submodule for this project

```shell
git submodule update --remote
```

2. Build this project 

```shell
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

Or use the `build.sh` script to build this project directly

```shell
./scripts/build.sh
```

## RDF data and Queries

Download the RDF data and queries that we want to use:
- 1 [Watdiv100M](https://mega.nz/folder/4r1iRCZZ#JKCi9mCCMKOaXadr73kDdQ)
- 2 [Wikidata](https://mega.nz/folder/5vUBHKTQ#TwpzwSzWhzniK1CeykxUCw)
- 3 [DBpedia](https://mega.nz/folder/luMREDjQ#hQpRCZGsRrfLhXMLwCHYpQ)

Our RDF engines benchmark is available [here](https://github.com/liuyipeng42/rdf_engines_benchmark)

## How to use

```
Usage: rdftdaa [COMMAND] [OPTIONS]

Commands:
  build      Build an RDF database.
  query      Query an RDF database.
  server     Start an RDF server.

Options:
  -h, --help  Show this help message and exit.

Commands:

  build
    Build an RDF database.

    Usage: rdftdaa build [OPTIONS]

    Options:
      -d, --database <NAME>   Specify the name of the database.
      -f, --file <FILE>       Specify the input file to build the database.
      -h, --help              Show this help message and exit.

  query
    Query an RDF database.

    Usage: rdftdaa query [OPTIONS]

    Options:
      -d, --database <NAME>   Specify the name of the database.
      -f, --file <FILE>       Specify the file containing the query.
      -h, --help              Show this help message and exit.

  server
    Start an RDF server.

    Usage: rdftdaa server [OPTIONS]

    Options:
      -d, --database <NAME>   Specify the name of the database.
      --ip <IP ADDRESS>       Specify the IP address for the server.
      --port <PORT>           Specify the port for the server.
      -h, --help              Show this help message and exit.
```
