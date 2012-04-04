-module(luwak_block).

-include("luwak.hrl").

-export([create/2,
         data/1,
         name/1]).

create(Riak, Data) ->
    Value = [
             {data, Data},
             {created, now()},
             {type, block}
            ],
    Obj = riakc_obj:new(?N_BUCKET, skerl:hexhash(?HASH_LEN, Data), Value),
    riakc_pb_socket:put(Riak, Obj,
                        [{w, 2}, {dw, 2}, return_body],
                        ?TIMEOUT_DEFAULT).

-spec data(list() | riakc_obj:riakc_obj()) -> binary().
data(Val) when is_list(Val) ->
    proplists:get_value(data, Val);
data(Object) ->
    proplists:get_value(data, binary_to_term(riakc_obj:get_value(Object))).

name(Object) ->
    riakc_obj:key(Object).
