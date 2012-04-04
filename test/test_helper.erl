-module(test_helper).

-export([setup/0, cleanup/1, riak_test/1]).

-define(APPS,
        [crypto,
         skerl,
         protobuffs,
         riakc]).

setup() ->
    load_and_start_apps(?APPS).

cleanup(_) ->
    ok.

riak_test(Fun) ->
  {ok, Riak} = riakc_pb_socket:start("127.0.0.1", 8087),
  Ret = (catch Fun(Riak)),
  case Ret of
    {'EXIT', Err} -> throw(Err);
    _ -> Ret
  end.

load_and_start_apps([]) -> ok;
load_and_start_apps([App|Tail]) ->
  ensure_loaded(App),
  ensure_started(App),
  load_and_start_apps(Tail).

ensure_loaded(App) ->
  case application:load(App) of
      ok ->
          ok;
      {error,{already_loaded,App}} ->
          ok;
      Error ->
          throw({"failed to load", App, Error})
  end.

ensure_started(App) ->
  case application:start(App) of
      ok ->
          ok;
      {error,{already_started,App}} ->
          ok;
      {error,{shutdown,_}} ->
          timer:sleep(250),
          ensure_started(App);
      Error ->
          throw({"failed to start", App, Error})
  end.
