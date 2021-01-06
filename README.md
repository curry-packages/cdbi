cdbi: Type-safe database programming
====================================

This package contains libraries to support type-safe database programming.
[This paper](http://dx.doi.org/10.4204/EPTCS.234.8)
contains a description of the basic ideas behind these libraries.

Since the current implementation is based on the
[SQLite](https://www.sqlite.org/) database system,
it must be installed together with the command line interface `sqlite3`.
This can be done in a Ubuntu distribution by

    sudo apt-get install sqlite3

Usually, it is not necessary to use these libraries directly.
Instead, one can use the Curry preprocessor to formulate
type-safe SQL queries which are translated into calls to these libraries.

--------------------------------------------------------------------------
