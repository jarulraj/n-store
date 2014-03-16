# HTM Based Multi-key Transactions 

## Concurrency Control

Evaluation of HTM support for concurrency control for `multi-key transactions` in key-value stores.

## Dependencies

- **g++ 4.8+** (for transactional memory support)
- **autoconf** (`apt-get install autoconf libtool`) 

## Setup
        
1. Fork it.        
2. Bootstrap, configure and build.
                                  
```
    ./bootstrap
    ./configure CXX=/usr/bin/g++-4.8 CXXFLAGS="-march=native -fgnu-tm -mrtm -mhle"
    make
```

## Test

**Example**    

```
./tester/main 
```
- get usage message

```
./tester/main -s1 -t16 -o16 -kzipf rtm
```
- runs 16 client threads that access the key-value store managed by a RTM-based concurrency controller
- a zipf distribution is followed by the keys accessed by the requests
- each transaction has 16 read/write operations (the ratio is configurable : default is 1:1)

