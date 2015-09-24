-module(blcs_file_backend).

-include_lib("lager/include/lager.hrl").

-behaviour(gen_event).

-export([init/1, handle_call/2, handle_event/2, handle_info/2, terminate/2,
        code_change/3]).

-export([config_to_id/1]).

-define(DEFAULT_LOG_LEVEL, info).
-define(DEFAULT_ROTATION_SIZE, 10485760). %% 10mb
-define(DEFAULT_ROTATION_DATE, "$D0"). %% midnight
-define(DEFAULT_ROTATION_COUNT, 5).
-define(DEFAULT_SYNC_LEVEL, error).
-define(DEFAULT_SYNC_INTERVAL, 1000).
-define(DEFAULT_SYNC_SIZE, 1024*64). %% 64kb
-define(DEFAULT_CHECK_INTERVAL, 1000).

-record(state, {
        name :: string(),
        level :: {'mask', integer()},
        %% tianwanli01
        fd :: pid(),
        %%fd :: file:io_device(),
        inode :: integer(),
        flap=false :: boolean(),
        size = 0 :: integer(),
        date :: undefined | string(),
        count = 10 :: integer(),
        shaper :: lager_shaper(),
        formatter :: atom(),
        formatter_config :: any(),
        sync_on :: {'mask', integer()},
        check_interval = ?DEFAULT_CHECK_INTERVAL :: non_neg_integer(),
        sync_interval = ?DEFAULT_SYNC_INTERVAL :: non_neg_integer(),
        sync_size = ?DEFAULT_SYNC_SIZE :: non_neg_integer(),
        last_check = os:timestamp() :: erlang:timestamp()
    }).

-type option() :: {file, string()} | {level, lager:log_level()} |
                  {size, non_neg_integer()} | {date, string()} |
                  {count, non_neg_integer()} | {high_water_mark, non_neg_integer()} |
                  {sync_interval, non_neg_integer()} |
                  {sync_size, non_neg_integer()} | {sync_on, lager:log_level()} |
                  {check_interval, non_neg_integer()} | {formatter, atom()} |
                  {formatter_config, term()}.

-spec init([option(),...]) -> {ok, #state{}} | {error, bad_config}.
init({FileName, LogLevel}) when is_list(FileName), is_atom(LogLevel) ->
    %% backwards compatability hack
    init([{file, FileName}, {level, LogLevel}]);
init({FileName, LogLevel, Size, Date, Count}) when is_list(FileName), is_atom(LogLevel) ->
    %% backwards compatability hack
    init([{file, FileName}, {level, LogLevel}, {size, Size}, {date, Date}, {count, Count}]);
init([{FileName, LogLevel, Size, Date, Count}, {Formatter,FormatterConfig}]) when is_list(FileName), is_atom(LogLevel), is_atom(Formatter) ->
    %% backwards compatability hack
    init([{file, FileName}, {level, LogLevel}, {size, Size}, {date, Date}, {count, Count}, {formatter, Formatter}, {formatter_config, FormatterConfig}]);
init([LogFile,{Formatter}]) ->
    %% backwards compatability hack
    init([LogFile,{Formatter,[]}]);
init([{FileName, LogLevel}, {Formatter,FormatterConfig}]) when is_list(FileName), is_atom(LogLevel), is_atom(Formatter) ->
    %% backwards compatability hack
    init([{file, FileName}, {level, LogLevel}, {formatter, Formatter}, {formatter_config, FormatterConfig}]);
init(LogFileConfig) when is_list(LogFileConfig) ->
    case validate_logfile_proplist(LogFileConfig) of
        false ->
            %% falied to validate config
            {error, {fatal, bad_config}};
        Config ->
            %% probabably a better way to do this, but whatever
            [RelName, Level, Date, Size, Count, HighWaterMark, SyncInterval, SyncSize, SyncOn, CheckInterval, Formatter, FormatterConfig] =
              [proplists:get_value(Key, Config) || Key <- [file, level, date, size, count, high_water_mark, sync_interval, sync_size, sync_on, check_interval, formatter, formatter_config]],
            Name = lager_util:expand_path(RelName),
            schedule_rotation(Name, Date),
            Shaper = #lager_shaper{hwm=HighWaterMark},
            State0 = #state{name=Name, level=Level, size=Size, date=Date, count=Count, shaper=Shaper, formatter=Formatter,
                formatter_config=FormatterConfig, sync_on=SyncOn, sync_interval=SyncInterval, sync_size=SyncSize,
                check_interval=CheckInterval},
            State = case blcs_util:open_logfile(Name, {SyncSize, SyncInterval}) of
                {ok, {FD, Inode, _}} ->
                    State0#state{fd=FD, inode=Inode};
                {error, Reason} ->
                    ?INT_LOG(error, "Failed to open log file ~s with error ~s", [Name, file:format_error(Reason)]),
                    State0#state{flap=true}
            end,
            {ok, State}
    end.

%% @private
handle_call({set_loglevel, Level}, #state{name=Ident} = State) ->
    case validate_loglevel(Level) of
        false ->
            {ok, {error, bad_loglevel}, State};
        Levels ->
            ?INT_LOG(notice, "Changed loglevel of ~s to ~p", [Ident, Level]),
            {ok, ok, State#state{level=Levels}}
    end;
handle_call(get_loglevel, #state{level=Level} = State) ->
    {ok, Level, State};
handle_call({set_loghwm, Hwm}, #state{shaper=Shaper, name=Name} = State) ->
    case validate_logfile_proplist([{file, Name}, {high_water_mark, Hwm}]) of
        false ->
            {ok, {error, bad_log_hwm}, State};
        _ ->
            NewShaper = Shaper#lager_shaper{hwm=Hwm},
            ?INT_LOG(notice, "Changed loghwm of ~s to ~p", [Name, Hwm]),
            {ok, {last_loghwm, Shaper#lager_shaper.hwm}, State#state{shaper=NewShaper}}
    end;
handle_call(_Request, State) ->
    {ok, ok, State}.

%% @private
handle_event({log, Message},
    #state{name=Name, level=L, shaper=Shaper, formatter=Formatter,formatter_config=FormatConfig} = State) ->
    case lager_util:is_loggable(Message,L,{lager_file_backend, Name}) of
        true ->
            case lager_util:check_hwm(Shaper) of
                {true, Drop, #lager_shaper{hwm=Hwm} = NewShaper} ->
                    NewState = case Drop > 0 of
                        true ->
                            Report = io_lib:format(
                                "lager_file_backend dropped ~p messages in the last second that exceeded the limit of ~p messages/sec", 
                                [Drop, Hwm]),
                            ReportMsg = lager_msg:new(Report, warning, [], []),
                            write(State, lager_msg:timestamp(ReportMsg),
                                lager_msg:severity_as_int(ReportMsg), Formatter:format(ReportMsg, FormatConfig));
                        false ->
                            State
                    end,
                    {ok,write(NewState#state{shaper=NewShaper},
                        lager_msg:timestamp(Message), lager_msg:severity_as_int(Message),
                        Formatter:format(Message,FormatConfig))};
                {false, _, NewShaper} ->
                    {ok, State#state{shaper=NewShaper}}
            end;
        false ->
            {ok, State}
    end;
handle_event(_Event, State) ->
    {ok, State}.

%% @private
handle_info({rotate, File}, #state{name=File,count=Count,date=Date} = State) ->
    _ = lager_util:rotate_logfile(File, Count),
    schedule_rotation(File, Date),
    {ok, State};
handle_info(_Info, State) ->
    {ok, State}.

%% @private
terminate(_Reason, #state{fd=FD}) ->
    %% flush and close any file handles
    %% tianwanli01
    _ = log_process:file_sync(FD),
    _ = log_process:file_close(FD),
    %%_ = file:datasync(FD),
    %%_ = file:close(FD),
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Convert the config into a gen_event handler ID
config_to_id({Name,_Severity}) when is_list(Name) ->
    {?MODULE, Name};
config_to_id({Name,_Severity,_Size,_Rotation,_Count}) ->
    {?MODULE, Name};
config_to_id([{Name,_Severity,_Size,_Rotation,_Count}, _Format]) ->
    {?MODULE, Name};
config_to_id([{Name,_Severity}, _Format]) when is_list(Name) ->
    {?MODULE, Name};
config_to_id(Config) ->
    case proplists:get_value(file, Config) of
        undefined ->
            erlang:error(no_file);
        File ->
            {?MODULE, File}
    end.


write(#state{name=Name, fd=FD, inode=Inode, flap=Flap, size=RotSize,
        count=Count} = State, Timestamp, Level, Msg) ->
    LastCheck = timer:now_diff(Timestamp, State#state.last_check) div 1000,
    case LastCheck >= State#state.check_interval orelse FD == undefined of
        true ->
            %% need to check for rotation
            case blcs_util:ensure_logfile(Name, FD, Inode, {State#state.sync_size, State#state.sync_interval}) of
                {ok, {_, _, Size}} when RotSize /= 0, Size > RotSize ->
                    case lager_util:rotate_logfile(Name, Count) of
                        ok ->
                            %% go around the loop again, we'll do another rotation check and hit the next clause of ensure_logfile
                            write(State, Timestamp, Level, Msg);
                        {error, Reason} ->
                            case Flap of
                                true ->
                                    State;
                                _ ->
                                    ?INT_LOG(error, "Failed to rotate log file ~s with error ~s", [Name, file:format_error(Reason)]),
                                    State#state{flap=true}
                            end
                    end;
                {ok, {NewFD, NewInode, _}} ->
                    %% update our last check and try again
                    do_write(State#state{last_check=Timestamp, fd=NewFD, inode=NewInode}, Level, Msg);
                {error, Reason} ->
                    case Flap of
                        true ->
                            State;
                        _ ->
                            ?INT_LOG(error, "Failed to reopen log file ~s with error ~s", [Name, file:format_error(Reason)]),
                            State#state{flap=true}
                    end
            end;
        false ->
            do_write(State, Level, Msg)
    end.

do_write(#state{fd=FD, name=Name, flap=Flap} = State, Level, Msg) ->
    %% delayed_write doesn't report errors
    %% tianwanli01
    _ = log_process:file_write(FD, unicode:characters_to_binary(Msg)),
    %%_ = file:write(FD, unicode:characters_to_binary(Msg)),
    {mask, SyncLevel} = State#state.sync_on,
    case (Level band SyncLevel) /= 0 of
        true ->
            %% force a sync on any message that matches the 'sync_on' bitmask
            %% tianwanli01
            Flap2 = case log_process:file_sync(FD) of
            %%Flap2 = case file:datasync(FD) of
                {error, Reason2} when Flap == false ->
                    ?INT_LOG(error, "Failed to write log message to file ~s: ~s",
                        [Name, file:format_error(Reason2)]),
                    true;
                ok ->
                    false;
                _ ->
                    Flap
            end,
            State#state{flap=Flap2};
        _ ->
            State
    end.

validate_loglevel(Level) ->
    try lager_util:config_to_mask(Level) of
        Levels ->
            Levels
    catch
        _:_ ->
            false
    end.

validate_logfile_proplist(List) ->
    try validate_logfile_proplist(List, []) of
        Res ->
            case proplists:get_value(file, Res) of
                undefined ->
                    ?INT_LOG(error, "Missing required file option", []),
                    false;
                _File ->
                    %% merge with the default options
                    {ok, DefaultRotationDate} = lager_util:parse_rotation_date_spec(?DEFAULT_ROTATION_DATE),
                    lists:keymerge(1, lists:sort(Res), lists:sort([
                            {level, validate_loglevel(?DEFAULT_LOG_LEVEL)}, {date, DefaultRotationDate},
                            {size, ?DEFAULT_ROTATION_SIZE}, {count, ?DEFAULT_ROTATION_COUNT},
                            {sync_on, validate_loglevel(?DEFAULT_SYNC_LEVEL)}, {sync_interval, ?DEFAULT_SYNC_INTERVAL},
                            {sync_size, ?DEFAULT_SYNC_SIZE}, {check_interval, ?DEFAULT_CHECK_INTERVAL},
                            {formatter, lager_default_formatter}, {formatter_config, []}
                        ]))
            end
    catch
        {bad_config, Msg, Value} ->
            ?INT_LOG(error, "~s ~p for file ~p",
                [Msg, Value, proplists:get_value(file, List)]),
            false
    end.

validate_logfile_proplist([], Acc) ->
    Acc;
validate_logfile_proplist([{file, File}|Tail], Acc) ->
    %% is there any reasonable validation we can do here?
    validate_logfile_proplist(Tail, [{file, File}|Acc]);
validate_logfile_proplist([{level, Level}|Tail], Acc) ->
    case validate_loglevel(Level) of
        false ->
            throw({bad_config, "Invalid loglevel", Level});
        Res ->
            validate_logfile_proplist(Tail, [{level, Res}|Acc])
    end;
validate_logfile_proplist([{size, Size}|Tail], Acc) ->
    case Size of
        S when is_integer(S), S >= 0 ->
            validate_logfile_proplist(Tail, [{size, Size}|Acc]);
        _ ->
            throw({bad_config, "Invalid rotation size", Size})
    end;
validate_logfile_proplist([{count, Count}|Tail], Acc) ->
    case Count of
        C when is_integer(C), C >= 0 ->
            validate_logfile_proplist(Tail, [{count, Count}|Acc]);
        _ ->
            throw({bad_config, "Invalid rotation count", Count})
    end;
validate_logfile_proplist([{high_water_mark, HighWaterMark}|Tail], Acc) ->
    case HighWaterMark of
        Hwm when is_integer(Hwm), Hwm >= 0 ->
            validate_logfile_proplist(Tail, [{high_water_mark, Hwm}|Acc]);
        _ ->
            throw({bad_config, "Invalid high water mark", HighWaterMark})
    end;
validate_logfile_proplist([{date, Date}|Tail], Acc) ->
    case lager_util:parse_rotation_date_spec(Date) of
        {ok, Spec} ->
            validate_logfile_proplist(Tail, [{date, Spec}|Acc]);
        {error, _} when Date == "" ->
            %% legacy config allowed blanks
            validate_logfile_proplist(Tail, [{date, undefined}|Acc]);
        {error, _} ->
            throw({bad_config, "Invalid rotation date", Date})
    end;
validate_logfile_proplist([{sync_interval, SyncInt}|Tail], Acc) ->
    case SyncInt of
        Val when is_integer(Val), Val >= 0 ->
            validate_logfile_proplist(Tail, [{sync_interval, Val}|Acc]);
        _ ->
            throw({bad_config, "Invalid sync interval", SyncInt})
    end;
validate_logfile_proplist([{sync_size, SyncSize}|Tail], Acc) ->
    case SyncSize of
        Val when is_integer(Val), Val >= 0 ->
            validate_logfile_proplist(Tail, [{sync_size, Val}|Acc]);
        _ ->
            throw({bad_config, "Invalid sync size", SyncSize})
    end;
validate_logfile_proplist([{check_interval, CheckInt}|Tail], Acc) ->
    case CheckInt of
        Val when is_integer(Val), Val >= 0 ->
            validate_logfile_proplist(Tail, [{check_interval, Val}|Acc]);
        always ->
            validate_logfile_proplist(Tail, [{check_interval, 0}|Acc]);
        _ ->
            throw({bad_config, "Invalid check interval", CheckInt})
    end;
validate_logfile_proplist([{sync_on, Level}|Tail], Acc) ->
    case validate_loglevel(Level) of
        false ->
            throw({bad_config, "Invalid sync on level", Level});
        Res ->
            validate_logfile_proplist(Tail, [{sync_on, Res}|Acc])
    end;
validate_logfile_proplist([{formatter, Fmt}|Tail], Acc) ->
    case is_atom(Fmt) of
        true ->
            validate_logfile_proplist(Tail, [{formatter, Fmt}|Acc]);
        false ->
            throw({bad_config, "Invalid formatter module", Fmt})
    end;
validate_logfile_proplist([{formatter_config, FmtCfg}|Tail], Acc) ->
    case is_list(FmtCfg) of
        true ->
            validate_logfile_proplist(Tail, [{formatter_config, FmtCfg}|Acc]);
        false ->
            throw({bad_config, "Invalid formatter config", FmtCfg})
    end;
validate_logfile_proplist([Other|_Tail], _Acc) ->
    throw({bad_config, "Invalid option", Other}).

schedule_rotation(_, undefined) ->
    ok;
schedule_rotation(Name, Date) ->
    erlang:send_after(lager_util:calculate_next_rotation(Date) * 1000, self(), {rotate, Name}),
    ok.
