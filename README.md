# cdb


An implementation of [cdb](http://cr.yp.to/cdb.html) written in erlang. The binary format is
compatible with D. J. Bernstein cdb as well as [tinycdb](http://www.corpit.ru/mjt/tinycdb.html).

### Build

```bash
    $ rebar3 compile
```

### Create a cdb

```erlang
    A = cdb:make_start("foo.cdb"),
    B = cdb:make_add(A, <<199919:64>>, <<"ccc">>),
    C = cdb:make_add(B, <<"aa">>, <<"bbb">>),
    ok = cdb:make_finish(C),
    ?assertEqual({<<199919:64>>, <<"ccc">>}, cdb:find("foo.cdb", <<199919:64>>)),
    ?assertEqual({<<"aa">>, <<"bbb">>}, cdb:find("foo.cdb", <<"aa">>)).
```

### Find a key

```erlang
    A = cdb:make_start("foo.cdb"),
    B = cdb:make_add(A, <<"aa">>, <<"bbb">>),
    ok = cdb:make_finish(B),
    ?assertEqual({<<"aa">>, <<"bbb">>}, cdb:find("foo.cdb", <<"aa">>)).
```
