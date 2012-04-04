-module(luwak_get_stream).

-export([start/4,
         recv/2,
         close/1]).

-record(map, {riak,blocksize,ref,pid,offset,endoffset}).

-include("luwak.hrl").

%% @spec start(Riak :: riak(), File :: luwak_file(),
%%             Start :: int(), Length :: length()) ->
%%        get_stream()
%% @doc Creates and returns a handle to a streaming get.  Initiating
%%      this call will cause the requested datablock ranges to be
%%      delivered as a set of messages to the calling process.
start(Riak, File, Start, Length) ->
    Ref = make_ref(),
    BlockSize = luwak_file:get_property(File, block_size),
    Root = luwak_file:get_property(File, root),
    MapStart = [{Root, 0}],
    Map = #map{riak=Riak,blocksize=BlockSize,ref=Ref,
               offset=Start,endoffset=Start+Length},
    Receiver = proc_lib:spawn(receiver_fun(MapStart, self(), Map)),
    {get_stream, Ref, Receiver}.

%% @spec recv(Stream :: get_stream(), Timeout :: int()) ->
%%        {binary(), int()} | eos | closed | {error, timeout}
%% @doc Receive will block the calling process until either the next
%%      data block has been delivered, the stream ends, or until
%%      Timeout milliseconds have elapsed.  Whichever occurs first.
%%      The data blocks are returned as a tuple with the data binary
%%      and its offset from the start of the file.
recv({get_stream, Ref, Pid}, Timeout) ->
    receive
        {get, Ref, Data, Offset} -> {Data, Offset};
        {get, Ref, eos} -> eos;
        {get, Ref, closed} -> closed;
        {'DOWN', _Mon, process, Pid, _Reason} -> closed
    after Timeout ->
            {error, timeout}
    end.

%% @doc Closes a get stream.  Use this to stop the flow of messages
%%      from a get stream.  It is not required that a completed stream
%%      have close called on it.
close({get_stream, _Ref, Pid}) ->
    exit(Pid, kill).

receiver_fun(MapInput, Parent, Map=#map{offset=Offset,
                                        endoffset=EndOffset,ref=Ref}) ->
    fun() ->
            ?debugFmt("receiver_fun(~p, ~p, ~p)~n", [MapInput, Parent, Map]),
            Self = self(),
            proc_lib:spawn(
              fun() -> tree_walk(MapInput, Map#map{pid=Self}) end),
            receive_loop(Ref, Offset, EndOffset, Parent)
    end.

receive_loop(Ref, Offset, EndOffset, Parent) when Offset >= EndOffset ->
    ?debugMsg("receive_loop sending eos~n"),
    Parent ! {get, Ref, eos},
    ok;
receive_loop(Ref, Offset, EndOffset, Parent) ->
    receive
        {get, Ref, Data, Offset} ->
            ?debugFmt("receive_loop got ~p~n", [{get, Ref, Data, Offset}]),
            Parent ! {get, Ref, Data, Offset},
            receive_loop(Ref, Offset+byte_size(Data), EndOffset, Parent);
        {eos, Ref} ->
            ?debugMsg("receive_loop got eos~n"),
            Parent ! {get, Ref, eos},
            ok;
        {close, Ref} ->
            ?debugFmt("receive_loop got ~p~n", [{close, Ref}]),
            Parent ! {get, Ref, closed},
            ok
    end.

tree_walk([], #map{pid=P, ref=Ref}) ->
    ?debugMsg("tree_walk([]) -> eos~n"),
    P ! {eos, Ref};
tree_walk([{_Key, Offset}|_Rest], #map{endoffset=End, pid=P, ref=Ref})
  when Offset > End ->
    ?debugFmt("tree_walk(~p > ~p) -> eos~n", [Offset, End]),
    P ! {eos, Ref};
tree_walk([{Key, Offset}|Rest], #map{riak=Riak}=Map) ->
    ?debugFmt("tree_walk(~p blocks)~n", [1+length(Rest)]),
    {ok, Obj} = riakc_pb_socket:get(Riak, ?N_BUCKET, Key, [{r, 1}]),
    Result = map(binary_to_term(riakc_obj:get_value(Obj)), Offset, Map),
    tree_walk(Result++Rest, Map).

map(Parent=#n{}, TreeOffset, 
    _Map=#map{riak=Riak,offset=Offset,endoffset=EndOffset, blocksize=BlockSize}) ->
    ?debugFmt("A map(~p, ~p, ~p)~n", [Parent, TreeOffset, _Map]),
    Fun = fun({Name,Length},AccOffset) ->
                  {[{Name, AccOffset}], AccOffset+Length}
          end,
    luwak_tree:get_range(Riak, Fun, Parent, BlockSize,
                         TreeOffset, Offset, EndOffset);
map(Block, TreeOffset,
    _Map=#map{offset=Offset,ref=Ref,pid=Pid,endoffset=EndOffset,blocksize=
BlockSize})
  when TreeOffset < Offset ->
    ?debugFmt("B map(~p, ~p, ~p)~n", [Block, TreeOffset, _Map]),
    PartialSize = Offset - TreeOffset,
    <<_:PartialSize/binary, Tail/binary>> = luwak_block:data(Block),
    case BlockSize >= EndOffset - TreeOffset of
        %% should be the same as BlockSize >= EndOffset-Offset
        false ->
            %% wanted the rest of the block
            ?debugFmt("sending ~p~n", [{get, Ref, Tail, Offset}]),
            Pid ! {get, Ref, Tail, Offset};
        true ->
            %% wanted only a middle chunk of the block
            SubPartialSize = EndOffset-Offset,
            <<SubTail:SubPartialSize/binary, _/binary>> = Tail,
            ?debugFmt("sending ~p~n", [{get, Ref, SubTail, Offset}]),
            Pid ! {get, Ref, SubTail, Offset}
    end,
    [];
map(Block, TreeOffset,
    _Map=#map{endoffset=EndOffset,ref=Ref,pid=Pid,blocksize=BlockSize})
  when BlockSize >= EndOffset - TreeOffset ->
    ?debugFmt("C map(~p, ~p, ~p)~n", [Block, TreeOffset, _Map]),
    case EndOffset - TreeOffset of
        PartialSize when PartialSize > 0 ->
            <<PartialData:PartialSize/binary, _/binary>> = luwak_block:data(Block),
            ?debugFmt("sending ~p~n", [{get, Ref, PartialData, TreeOffset}]),
            Pid ! {get, Ref, PartialData, TreeOffset};
        _PartialSize ->
            %% boundary case where this block was looked up, but not needed
            ok
    end,
    [];
map(Block, TreeOffset, _Map=#map{ref=Ref,pid=Pid}) ->
    ?debugFmt("D map(~p, ~p, ~p)~n", [Block, TreeOffset, _Map]),
    Data = luwak_block:data(Block),
    ?debugFmt("sending ~p~n", [{get, Ref, Data, TreeOffset}]),
    Pid ! {get, Ref, Data, TreeOffset},
    [].
  
