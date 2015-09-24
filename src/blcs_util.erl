-module(blcs_util).

-include_lib("kernel/include/file.hrl").

-export([
        open_logfile/2, 
        ensure_logfile/4,
        log_process_name/1
]).


open_logfile(Name, Buffer) ->
    case filelib:ensure_dir(Name) of
        ok ->
            Options = [append, raw] ++
            case  Buffer of
                {Size, Interval} when is_integer(Interval), Interval >= 0, is_integer(Size), Size >= 0 ->
                    [{delayed_write, Size, Interval}];
                _ -> []
            end,
            %% tianwanli01
            case log_process:file_open(Name, Options) of 
                {ok, _, FD} ->
                    io:format("blcs log pid : ~p~n", [FD]),
                    case file:read_file_info(Name) of
                        {ok, FInfo} ->
                            Inode = FInfo#file_info.inode,
                            {ok, {FD, Inode, FInfo#file_info.size}};
                        X -> X
                    end;
                Y -> 
                    io:format("Create process erorr: ~p~n", [Y]),
                    Y
            end;
        Z -> Z
    end.

ensure_logfile(Name, FD, Inode, Buffer) ->
    case file:read_file_info(Name) of
        {ok, FInfo} ->
            Inode2 = FInfo#file_info.inode,
            case Inode == Inode2 of
                true ->
                    {ok, {FD, Inode, FInfo#file_info.size}};
                false ->
                    %% tianwnali01
                    _ =log_process:file_close(FD),
                    case open_logfile(Name, Buffer) of
                        {ok, {FD2, Inode3, Size}} ->
                            %% inode changed, file was probably moved and
                            %% recreated
                            {ok, {FD2, Inode3, Size}};
                        Error ->
                            Error
                    end
            end;
        _ ->
            %% tianwanli01
            _ = log_process:file_close(FD),
            case open_logfile(Name, Buffer) of
                {ok, {FD2, Inode3, Size}} ->
                    %% file was removed
                    {ok, {FD2, Inode3, Size}};
                Error ->
                    Error
            end
    end.

log_process_name(FileName)when erlang:is_binary(FileName) ->
    binary_to_list(FileName);
log_process_name(FileName) when erlang:is_list(FileName) ->
    list_to_atom("log_process_" ++ integer_to_list(erlang:phash2(FileName))).
