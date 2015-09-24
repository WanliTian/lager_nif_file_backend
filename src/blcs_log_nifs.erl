-module(blcs_log_nifs).

-export([
        file_open/2,
        file_close/1,
        file_pread/3,
        file_pwrite/3,
        file_read/2,
        file_write/2,
        file_sync/1,
        file_position/2,
        file_seekbof/1,
        file_truncate/1
]).

-on_load(init/0).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

init() ->
    case code:priv_dir(blcs_log) of
        {error, bad_name} ->
            case code:which(?MODULE) of
                FileName when is_list(FileName) ->
                    SoName = filename:join([filename:dirname(FileName), "../priv", "blcs_log"]);
                _ ->
                    SoName = filename:join("../priv", "blcs_log")
            end;
        Dir ->
            SoName = filename:join(Dir, "blcs_log")
    end,
    erlang:load_nif(SoName, 0).

file_open(Filename, Opts) ->
    big(),
    file_open_int(Filename, Opts).
file_open_int(_FileName, _Opts) ->
    erlang:nif_error({error, not_loaded}).

file_close(Ref) ->
    big(),
    file_close_int(Ref).

file_close_int(_Ref) ->
    erlang:nif_error({error, not_loaded}).

file_pread(Ref, Offset, Size) ->
    big(),
    file_pread_int(Ref, Offset, Size).

file_pread_int(_Ref, _Offset, _Size) ->
    erlang:nif_error({error, not_loaded}).

file_pwrite(Ref, Offset, Bytes) ->
    big(),
    file_pwrite_int(Ref, Offset, Bytes).

file_pwrite_int(_Ref, _Offset, _Bytes) ->
    erlang:nif_error({error, not_loaded}).

file_read(Ref, Size) ->
    big(),
    file_read_int(Ref, Size).

file_read_int(_Ref, _Size) ->
    erlang:nif_error({error, not_loaded}).

file_write(Ref, Bytes) ->
    big(),
    file_write_int(Ref, Bytes).

file_write_int(_Ref, _Bytes) ->
    erlang:nif_error({error, not_loaded}).

file_sync(Ref) ->
    big(),
    file_sync_int(Ref).

file_sync_int(_Ref) ->
    erlang:nif_error({error, not_loaded}).

file_position(Ref, Position) ->
    big(),
    file_position_int(Ref, Position).

file_position_int(_Ref, _Position) ->
    erlang:nif_error({error, not_loaded}).

file_seekbof(Ref) ->
    big(),
    file_seekbof_int(Ref).

file_seekbof_int(_Ref) ->
    erlang:nif_error({error, not_loaded}).

file_truncate(Ref) ->
    big(),
    file_truncate_int(Ref).

file_truncate_int(_Ref) ->
    erlang:nif_error({error, not_loaded}).

%% ========================================
%%       Internal Functions
%% ========================================
big() ->
    erlang:bump_reductions(1900).

%%small() ->
%%    erlang:bump_reductions(500).

%% ========================================
%%     EUnit Test 
%% ========================================
-ifdef(TEST).
fileopts_test()->
    Binary = <<"hello world">>,
    Size   = erlang:size(Binary),
    {ok, Res} = 
    case file:read_file_info("./log_test") of
        {ok, _} ->
            {ok, ResI} = file_open("./log_test", []),
            file_truncate(ResI),
            {ok, ResI};
        _ ->
            file_open("./log_test", [create])
    end,
    file_write(Res, Binary),
    {ok, P} = file_position(Res, {cur,0}),
    ?assertEqual(P, Size),
    {ok, _} = file_position(Res, {bof,0}),
    {ok, ReadBinary} = file_read(Res, Size),
    ?assertEqual(Binary, ReadBinary),
    file_close(Res).
-endif.
