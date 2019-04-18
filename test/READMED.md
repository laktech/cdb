cdb tests
=========

The file 1.cdb was created using the tinycdb-0.78 library.

```
$ echo "+3,4:one->here
+1,1:a->b
+1,3:b->abc
+3,4:one->also

" | ./cdb -c 1.cdb
$ cdb -q 1.cdb one
"here"
```

This file can be used to test the dump tool.

