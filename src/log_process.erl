-module(log_process).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    start_link/1,
    file_open/2,
    file_close/1,
    file_write/2,
    file_sync/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link({FilePath, _}=Args) ->
    LogProcessName = blcs_util:log_process_name(FilePath),
    io:format("gen_server_name: ~p~n", [LogProcessName]),
    %%gen_server:start_link({local, LogProcessName}, ?MODULE, Args, []).
    case gen_server:start_link({local, LogProcessName}, ?MODULE, Args, []) of 
        {ok, Pid} ->
            io:format("....................ok,,,,,,,,,,,,,,,,,,,,,,~n"),
            {ok, Pid, LogProcessName};
        Other ->
            io:format("....................ok,,,,,,,,,,,,,,,,,,,,,,~p~n", [Other]),
            Other
    end.

file_open(FilePath, Options)->
    Res = supervisor:start_child(log_process_sup, [{FilePath, Options}]),
    io:format(".....................Res:~p~n", [Res]),
    Res.

file_close(Pid) ->
    gen_server:call(Pid, close).

file_write(Pid, Log) ->
    gen_server:call(Pid, {write, Log}).

file_sync(Pid) ->
    gen_server:call(Pid, sync).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init({FilePath, Options}) ->
    case blcs_log_nifs:file_open(FilePath, [create]) of
        {ok, Fd} ->
            Map = #{
                    "file_path" => FilePath,
                    "options"   => Options,
                    "fd"        => Fd,
                    "log"       => <<"">>
                },
            NewMap = maps:merge(Map, size_interval(Options)),
            case maps:get("interval", NewMap, undefined) of 
                undefined ->
                    ok;
                Interval ->
                    erlang:start_timer(Interval, self(), write)
            end,
            {ok, NewMap};
        Other ->
            io:format("stop..........~n"),
            {stop, Other}
    end.

handle_call({write, Log}, From, State) when is_list(Log) ->
    handle_call({write, list_to_binary(Log)}, From, State);
handle_call({write, Log}, _From, State) ->
    NewState = case maps:get("size", State, undefined) of
        undefined ->
            Fd = maps:get("fd", State),
            blcs_log_nifs:file_write(Fd, Log),
            State;
        Size ->
            LogBuffer = maps:get("log", State),
            FreshLogBuffer = <<LogBuffer/binary, Log/binary>>,
            case erlang:size(FreshLogBuffer) >= Size of 
                true ->
                    Fd = maps:get("fd", State),
                    FreshState = maps:put("log", <<"">>, State),
                    blcs_log_nifs:file_write(Fd, FreshLogBuffer),
                    FreshState;
                false ->
                    maps:put("log", FreshLogBuffer, State)
            end
    end,
    {reply, ok, NewState};
handle_call(sync, _From, State) ->
    Fd = maps:get("fd", State),
    NewState = case maps:get("log", State) of
        <<"">> ->
            State;
        Other ->
            blcs_log_nifs:file_write(Fd, Other),
            maps:put("log", <<"">>, State)
    end,
    blcs_log_nifs:file_sync(Fd),
    {reply, ok, NewState};
handle_call(close, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, _, write}, State) ->
    case maps:get("log", State, undefined) of
        undefined ->
            {noreply, State};
        Other ->
            Fd = maps:get("fd", State),
            Interval = maps:get("interval", State),
            blcs_log_nifs:file_write(Fd, Other),
            NewState=State#{"log" => <<"">>},
            erlang:start_timer(Interval, self(), write),
            {noreply, NewState}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    Fd = maps:get("fd", State),
    case maps:get("log", State) of 
        <<"">> ->
            ok;
        Other ->
            blcs_log_nifs:file_write(Fd, Other)
    end,
    blcs_log_nifs:file_close(Fd),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
size_interval(List) ->
    case erlang:length(List) >= 3 of 
        true ->
            {delayed_write, Size, Interval} = lists:nth(3, List),
            #{"size" => Size, "interval" => Interval};
        false ->
            #{}
    end.

-ifdef(TEST).
file_ops_test() ->
    application:ensure_all_started(blcs_log),
    FilePath = "/home/users/tianwanli01/Code/Erlang/blcs_log/" ++ "log_porcess.log",
    Options  = [append, raw, {delayed_write, 1000000,1000}],
    case file:read_file_info(FilePath) of 
        {ok, _} ->
            file:delete(FilePath);
        _ ->
            ok
    end,
    {ok, Fd} = log_process:file_open(FilePath, Options),
    ?assertEqual(ok, log_process:file_write(Fd, <<"ok">>)),
    ?assertEqual(ok, log_process:file_write(Fd, <<"cancel">>)),
    ?assertEqual(ok, log_process:file_sync(Fd)),
    ?assertEqual(ok, log_process:file_close(Fd)),

    {ok, NFD} = blcs_log_nifs:file_open(FilePath, [readonly]),
    ?assertEqual({ok, <<"okcancel">>}, blcs_log_nifs:file_read(NFD, 8)),
    blcs_log_nifs:file_close(NFD).
-endif.
