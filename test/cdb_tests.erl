%% MIT License
%%
%% Copyright (c) 2019 Matt Kowalczyk
%%
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
-module(cdb_tests).

-include_lib("eunit/include/eunit.hrl").

cdb_test_() -> [
		fun foo/0,
		{timeout, 120, fun collision/0}
	       ].

foo() ->
    A = cdb:make_start("foo.cdb"),
    B = cdb:make_add(A, <<"aa">>, <<"bbb">>),
    ok = cdb:make_finish(B),
    ?assertEqual({<<"aa">>, <<"bbb">>}, cdb:find("foo.cdb", <<"aa">>)).

%% 199919

collision() ->
    A = cdb:make_start("foo.cdb"),
    B = cdb:make_add(A, <<199919:64>>, <<"ccc">>),
    C = cdb:make_add(B, <<"aa">>, <<"bbb">>),
    ok = cdb:make_finish(C),
    ?assertEqual({<<199919:64>>, <<"ccc">>}, cdb:find("foo.cdb", <<199919:64>>)),
    ?assertEqual({<<"aa">>, <<"bbb">>}, cdb:find("foo.cdb", <<"aa">>)).

collision(0, Eq) ->
    not_found;
collision(N, Eq) ->
    Hval = cdb:hash(<<N:64>>),
    <<_:(32-8), Hslot:8>> = Hval,
    if 
	Hslot == Eq -> N;
	true -> collision(N-1,Eq)
    end.
