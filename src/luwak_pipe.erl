%% @doc Fitting for walking a luwak tree in a riak_pipe pipeline.
%%
%%      The point of this fitting is to recurse down a Luwak file
%%      tree, and send the data blocks at the leafs downstream for
%%      processing.
%%
%%      Inputs to this fitting are of the form `{Offset, NodeKey}',
%%      where `NodeKey' is the key of an object in the `?LUWAK_NODE'
%%      bucket, and `Offset' is the offset in the file that caused
%%      this tree to be traversed.  When `NodeKey' points to an
%%      intermediate Luwak tree node, the keys for the children of
%%      that node are passed back into this fitting as input.  When
%%      `NodeKey' points to a Luwak block, the data for that block is
%%      extracted and passed down the line as `{Offset, Data}'.
%%
%%      To easily send the proper input for the root of a luwak file,
%%      use {@link queue_file/2}.  That function will lookup the root
%%      node and send its key as input to the pipeline you specify.
%%
%%      The chashfun for this fitting should always be the {@link
%%      chashfun/1} exported from this module.  To avoid having to
%%      remember that, simply call {@link spec/0} to get the correct
%%      `#fitting_spec{}' record.
%%
%%      Using the {@link spec/0} and {@link queue_file} functions
%%      together to process a file looks something like this:
%%
%% ```
%% {ok, Pipe} = riak_pipe:exec([luwak_pipe:spec() | OtherPipeSpecs], []),
%% luwak_pipe:queue_file(Pipe, LuwakFilename),
%% luwak_pipe:eoi(Pipe).
%% '''
-module(luwak_pipe).
-behavior(riak_pipe_vnode_worker).

-export([
         spec/0, spec/1,
         queue_file/2,
         root_of_file/1,
         init/2,
         process/3,
         done/1,
         chashfun/1
        ]).

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include_lib("luwak/include/luwak.hrl").
-include_lib("riak_kv/include/riak_kv_vnode.hrl").

-record(state, {
          p :: riak_pipe_vnode:partition(),
          fd :: riak_pipe_fitting:details()
         }).

-type state() :: #state{}.

%%% Pipeline Setup Utilities

%% @equiv spec(luwak_tree_walk)
spec() ->
    spec(luwak_tree_walk).

%% @doc Produce a `#fitting_spec{}' record for including this fitting
%%      in a pipe.  This automatically sets the module, chashfun, and
%%      nval fields correctly.  The name field will be set to the
%%      `Name' passed as argument.
%%
%%      The q_limit field is set to the default order for luwak trees,
%%      to make the recursive descent less likely to drop inputs
%%      because of overflowing queues.  You may wish to change this
%%      field if you use an alternate tree order (or if profiling your
%%      pipeline suggests it's a good idea, of course).
-spec spec(term()) -> riak_pipe:fitting_spec().
spec(Name) ->
    #fitting_spec{name=Name,
                  module=?MODULE,
                  nval=n_bucket_nval(),
                  chashfun=fun chashfun/1,
                  q_limit=?ORDER_DEFAULT}.

%% @doc Get the nval for the Luwak node bucket.
-spec n_bucket_nval() -> non_neg_integer().
n_bucket_nval() ->
    {ok, C} = riak:local_client(),
    Bucket = C:get_bucket(?N_BUCKET),
    {n_val, NVal} = lists:keyfind(n_val, 1, Bucket),
    NVal.

%% @doc Find the key for the root node of the named file, and send the
%%      input `{0, RootNodeKey}' to the given pipe (this is the form
%%      of input expected by the fitting implemented by this module).
-spec queue_file(riak_pipe:pipe(), binary())
         -> ok | {error, riak_pipe_vnode:qerror() | root_undefined}.
queue_file(Pipe, LuwakFilename) ->
    case luwak_pipe:root_of_file(LuwakFilename) of
        undefined ->
            {error, root_undefined};
        Root ->
            riak_pipe:queue_work(Pipe, {0, Root})
    end.

%% @doc Find the key for the root node of the named file.
-spec root_of_file(binary()) -> binary().
root_of_file(Filename) ->
    {ok, C} = riak:local_client(),
    {ok, F} = luwak_file:get(C, Filename),
    luwak_file:get_property(F, root).

%%% Fitting Behavior

%% @doc Init just stashes the `Partition' and `FittingDetails' away,
%%      so this fitting can send outputs later.
-spec init(riak_pipe_vnode:partition(), riak_pipe_fitting:details()) ->
         {ok, state()}.
init(Partition, FittingDetails) ->
    {ok, #state{p=Partition, fd=FittingDetails}}.

%% @doc Process looks up the Riak KV object `{?N_BUCKET, Key}', which
%%      should be a Luwak node object.  If the node object is a Luwak
%%      block (leaf of the tree), then its data is sent to the
%%      downstream fitting as `{Offset, Data}'.  If the node object
%%      is, instead, a Luwak intermediate tree node, then the keys for
%%      the children it references are sent back as inputs to this
%%      fitting, to recursively walk down the tree.
%%
%%      Implementation note: Most of this function is identical to
%%      {@link //riak_kv/riak_kv_pipe_get}.  The recursive tree walk
%%      in `luwak_pipe` could not be implemented as the typical
%%      `riak_kv_pipe_get`->`riak_pipe_w_xform` sequence, because
%%      recursive inputs can only be sent to the current fitting, not
%%      upstream fittings
-spec process({non_neg_integer(), binary()}, boolean(), state()) ->
         {ok | forward_preflist, state()}.
process({Offset, Key}, _Last,
        #state{p=Partition, fd=FittingDetails}=State) ->
    ReqId = make_req_id(),
    riak_core_vnode_master:command(
      {Partition, node()}, %% assume local partfun was used
      ?KV_GET_REQ{bkey={?N_BUCKET, Key}, req_id=ReqId},
      {raw, ReqId, self()},
      riak_kv_vnode_master),
    receive
        {ReqId, {r, {ok, Obj}, _, _}} ->
            case riak_object:get_value(Obj) of
                #n{children=Children} ->
                    recurse(Children, Offset, State);
                Block ->
                    {data, Data} = lists:keyfind(data, 1, Block),
                    riak_pipe_vnode_worker:send_output(
                      {Offset, Data}, Partition, FittingDetails)
            end,
            {ok, State};
        {ReqId, {r, {error, _}, _, _}} ->
            {forward_preflist, State}
    end.    

%% Internal implementation of recursing on an intermediate node.
recurse([{Child, Size}|Rest], Offset, #state{p=P, fd=FD}=State) ->
    riak_pipe_vnode_worker:recurse_input({Offset, Child}, P, FD),
    recurse(Rest, Offset+Size, State);
recurse([], _Offset, _State) ->
    ok.

%% Unused.
done(_State) ->
    ok.

make_req_id() ->
    erlang:phash2(erlang:now()). % stolen from riak_client

%% @doc The hashing function required for this fitting.  It hashes on
%%      `{?N_BUCKET, Key}', just as Riak KV does to store Luwak nodes,
%%      so this fitting can assume that it's running on the same
%%      partition as the data it requires.
-spec chashfun({non_neg_integer(), binary()}) -> riak_pipe_vnode:chash().
chashfun({_Offset, Key}) ->
    riak_core_util:chash_key({?N_BUCKET, Key}).
