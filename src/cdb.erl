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

-module(cdb).

%% API exports
-export([cdb2list/1,list2cdb/2,make_start/1,make_add/3,make_finish/1,find/2,dump/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([hash/1,random_cdb/2]).
-endif.

-define(NUM_SLOT_BITS, 8).
-define(NUM_SLOTS, (1 bsl ?NUM_SLOT_BITS)).
-define(WORD_SIZE, 4).
-define(DWORD_SIZE, 8).
-define(DWORD_BITS, (?DWORD_SIZE * 8)).
-define(WORD_BITS, (?WORD_SIZE * 8)).
-define(HEADER_LEN, (?NUM_SLOTS * ?DWORD_SIZE)).

%%--------------------------------------------------------------------
%% Data Type: cdbmp
%% where:
%%    fd: The cdb file descriptor
%%    rl: an array is a fixed sized array of NUM_SLOTS buckets. Each bucket maintains a list
%%        of records added to the database that fall into the bucket. The bucket type is
%%        {hash(), pos} where hash() is the hash of the key and pos is the position of the
%%        key-value record in the database file.
%%--------------------------------------------------------------------

-record(cdbmp, {
	  fd :: file:io_device(),
	  rl = array:new([
			  {size, ?NUM_SLOTS},
			  {fixed, true},
			  {default, []}]) :: array:array(<<_:?DWORD_BITS>>)
	 }).

-type hash() :: <<_:?WORD_BITS>>.
-opaque cdbmp() :: #cdbmp{}.

-export_type([cdbmp/0]).

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of all key-value elements read from the cdb file
%% given by File.
%% @end
%%--------------------------------------------------------------------

-spec cdb2list(file:filename()) -> [{binary(), binary()}].
cdb2list(File) ->
    {ok, Fd} = file:open(File, [read, raw, binary, read_ahead]),
    {ok, <<Eod:?WORD_BITS/little>>} = file:read(Fd, ?WORD_SIZE),
    {ok, _NewPos} = file:position(Fd, ?HEADER_LEN),
    Rv = lists:reverse(cdb2list(Fd, Eod - ?HEADER_LEN, [])),
    file:close(Fd),
    Rv.

-spec dump(file:filename()) -> ok.
dump(File) ->
    foreach(File, fun(Klen,Vlen,K,V) -> io:put_chars(io_lib:format("+~p,~p:~s->~s~n", [Klen,Vlen,K,V])) end),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Writes a list of key-value tuples to a cdb database written to the
%% file specified by File.
%% @end
%%--------------------------------------------------------------------

-spec list2cdb([{binary(), binary()}], file:filename()) -> ok.
list2cdb(X, File) ->
    CdbMp = lists:foldl(fun({K,V}, Acc) -> make_add(Acc, K, V) end, make_start(File), X),
    make_finish(CdbMp).

%%--------------------------------------------------------------------
%% @doc
%% Create a builder for a cdb database backed by the file specified by File.
%% @end
%%--------------------------------------------------------------------

-spec make_start(file:filename()) -> cdbmp().
make_start(File) ->
    {ok, Fd} = file:open(File, [write, raw, binary, delayed_write]),
    {ok, _NewPos} = file:position(Fd, ?HEADER_LEN),
    #cdbmp{fd = Fd}.

%%--------------------------------------------------------------------
%% @doc
%% Create a builder for a cdb database backed by the file specified by File.
%% @end
%%--------------------------------------------------------------------

-spec make_add(cdbmp(), binary(), binary()) -> cdbmp().
make_add(CdbMp = #cdbmp{fd=Fd, rl=Rl}, Key, Val) ->
    % add hash entry
    {ok, Pos} = file:position(Fd, {cur, 0}),
    Hval = hash(Key),
    <<_:(32-?NUM_SLOT_BITS), Hslot:?NUM_SLOT_BITS>> = Hval,
    NewRl = array:set(Hslot, [{Hval, Pos}|array:get(Hslot, Rl)], Rl),

    % write record
    KeyLen = erlang:size(Key),
    ValLen = erlang:size(Val),
    ok = file:write(Fd, [<<KeyLen:?WORD_BITS/little>>,<<ValLen:?WORD_BITS/little>>, Key, Val]),

    CdbMp#cdbmp{rl = NewRl}.

%%--------------------------------------------------------------------
%% @doc
%% Commit the cdb builder and write it's content to the filesystem.
%% @end
%%--------------------------------------------------------------------

-spec make_finish(cdbmp()) -> ok.
make_finish(CdbMp=#cdbmp{fd=Fd}) ->
    Pl = fun(_I, Rl, []) ->
		 {ok, Pos} = file:position(Fd, {cur, 0}), [{Pos, make_hashtable(Rl)}];
	    (_I, Rl, Acc = [{PPos, {_PIoHash, PHLen}}|_]) ->
		 Pos = PPos + PHLen * ?DWORD_SIZE,
		 [{Pos, make_hashtable(Rl)}|Acc]
	    end,
    Rv = lists:reverse(array:foldl(Pl, [], CdbMp#cdbmp.rl)),
    file:pwrite(Fd, 0, <<<<Pos:?WORD_BITS/little, HLen:?WORD_BITS/little>>
			 || {Pos, {_IoHash, HLen}} <- Rv>>),
    file:pwrite(Fd, [{Pos,IoHash} || {Pos,{IoHash, HLen}} <- Rv, HLen > 0]),
    file:close(CdbMp#cdbmp.fd).


%%--------------------------------------------------------------------
%% @doc
%% Find the Key in the cdb backed by File. Returns a key-value tuple if the key exists or
%% not_found otherwise.
%% @end
%%--------------------------------------------------------------------

-spec find(file:filename(), binary()) -> not_found | {binary(), binary()}.
find(File, Key) ->
    {ok, Fd} = file:open(File, [read, raw, binary, read_ahead]),
    <<Hval:?WORD_BITS>> = hash(Key),
    {ok, <<Pos:?WORD_BITS/little, HLen:?WORD_BITS/little>>} =
	file:pread(Fd, (Hval band 16#FF) bsl 3, ?DWORD_SIZE),
    find(Fd, Key, Hval, Pos, HLen).

-spec find(file:io_device(),
	       binary(),
	       integer(),
	       non_neg_integer(),
	       non_neg_integer()) -> not_found | {binary(), binary()}.
find(_Fd, _Key, _Hval, _Pos, 0) ->
    not_found;
find(Fd, Key, Hval, Pos, HLen) ->
    {ok, <<EHval:?WORD_BITS/little, HPos:?WORD_BITS/little>>} = file:pread(Fd, Pos, ?DWORD_SIZE),
    if
	Hval == EHval->
	    try
		KLen = byte_size(Key),
		{ok, <<KLen:?WORD_BITS/little, VLen:?WORD_BITS/little>>} =
		    file:pread(Fd, HPos, ?DWORD_SIZE),
		{ok, <<Key:KLen/binary, Val:VLen/binary>>} =
		    file:pread(Fd, HPos + ?DWORD_SIZE, KLen + VLen),
		{Key, Val}
	    catch
		error:{badmatch,{ok,_}} -> find(Fd, Key, Hval, Pos + ?DWORD_SIZE, HLen - 1)
	    end;
	true ->
	    find(Fd, Key, Hval, Pos + ?DWORD_SIZE, HLen - 1)
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%%
%% make_hashtable returns the on-disk format of the hashtable. The
%% function returns an iolist of length NUM_SLOTS where each element
%% is the bitstring to be written to disk.
%%
%% It assigns to each record in Rl a slot in the hashtable. If a
%% slot is full, the next slot is tried.
%%
%% The size of the hashtable is always 2*RlLen.
%%
%% Rl      - the record list, elements are [{Hval, Pos}, ...].
%%
%% Returns an iolist of the form [<<Hval, Pos>>, ...] in little
%% endian format.
%%
%% @end
%%--------------------------------------------------------------------
-spec make_hashtable([{hash(), non_neg_integer()}]) -> {[<<_:?DWORD_BITS>>], non_neg_integer()}.
make_hashtable(Rl) ->
    HashLen = erlang:length(Rl) bsl 1,
    % IdxMN stores <<Hval, Pos>>, 8 bytes, and false if
    % the slot is empty.
    IdxM = lists:foldl(fun(Elem,IdxM) ->
			       make_hashtable(Elem, IdxM, 0, HashLen)
			end,
		       array:new([{size, HashLen}, {fixed, true}, {default, <<0:?DWORD_BITS>>}]),
		       Rl),
    {array:to_list(IdxM), HashLen}.

-spec make_hashtable({hash(), non_neg_integer()},
		     array:array(<<_:?DWORD_BITS>>),
		     non_neg_integer(),
		     non_neg_integer()) -> array:array(<<_:?DWORD_BITS>>).
make_hashtable(Elem={<<HInt:?WORD_BITS>>, Pos}, IdxMA, SlotPos, HashLen) ->
    Idx = ((HInt bsr ?NUM_SLOT_BITS) + SlotPos) rem HashLen,
    case array:get(Idx, IdxMA) of
	% slot was unoccupied
	<<0:?DWORD_BITS>> ->
	    array:set(Idx,
		      <<<<HInt:?WORD_BITS/little>>/binary, <<Pos:?WORD_BITS/little>>/binary>>,
		      IdxMA);

	% slot was occupied, search next position.
	_ -> make_hashtable(Elem, IdxMA, SlotPos+1, HashLen)
    end.

-spec hash(binary()) -> hash().
hash(B) ->
    hash(B, 5381).
hash(<<>>, Acc) ->
    <<Acc:?WORD_BITS>>;
hash(<<X, Xs/binary>>, Acc) ->
    hash(Xs, (((Acc bsl 5) + Acc) bxor X) band 16#FFFFFFFF).

-spec cdb2list(file:io_device(),
	       non_neg_integer(),
	       [{binary(), binary()}]) -> [{binary(), binary()}].
cdb2list(_Fd, 0, Acc) ->
    Acc;
cdb2list(Fd, Rem, Acc) ->
    {ok, <<Klen:?WORD_BITS/little, Vlen:?WORD_BITS/little>>} = file:read(Fd, ?DWORD_SIZE),
    DataLen = Klen + Vlen,
    {ok, <<K:Klen/binary, V:Vlen/binary>>} = file:read(Fd, DataLen),
    cdb2list(Fd, Rem - ?DWORD_SIZE - DataLen, [{K,V}|Acc]).

foreach(File, F) ->
    {ok, Fd} = file:open(File, [read, raw, binary, {read_ahead, 8192 * 2}]),
    {ok, <<Eod:?WORD_BITS/little>>} = file:read(Fd, ?WORD_SIZE),
    {ok, _NewPos} = file:position(Fd, ?HEADER_LEN),
    foreach(Fd, Eod - ?HEADER_LEN, F),
    file:close(Fd),
    ok.

foreach(_Fd, 0, _F) ->
    ok;
foreach(Fd, Rem, F) ->
    {ok, <<Klen:?WORD_BITS/little, Vlen:?WORD_BITS/little>>} = file:read(Fd, ?DWORD_SIZE),
    DataLen = Klen + Vlen,
    {ok, <<K:Klen/binary, V:Vlen/binary>>} = file:read(Fd, DataLen),
    _ = F(Klen,Vlen, K,V),
    foreach(Fd, Rem - ?DWORD_SIZE - DataLen, F).

%%====================================================================
%% private tests
%%====================================================================

-ifdef(TEST).
hash_test() ->
    ?assertEqual(hash(<<"aa">>), <<5860901:?WORD_BITS>>).

%%--------------------------------------------------------------------
%% @doc
%% Helper method to create a random db.
%% @end
%%--------------------------------------------------------------------
random_cdb(File, N) ->
    do_random_cdb(make_start(File), N).

do_random_cdb(Db, 0) ->
    make_finish(Db);
do_random_cdb(Db, N) ->
    {K, V} = do_random_kv(),
    do_random_cdb(make_add(Db, K, V), N - 1).

do_random_kv() ->
    KLen = 4 + rand:uniform(25),
    VLen = 25 + rand:uniform(250),
    {do_random_ascii_bin(KLen), do_random_ascii_bin(VLen)}.

do_random_ascii_bin(N) -> << <<(64 + rand:uniform(25)), (96 + rand:uniform(25))>>
			  || _X <- lists:seq(1,N) >>.
-endif.
