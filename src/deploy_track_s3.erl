%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% This module is responsible for collecting download information
%% from Amazon S3.
%%
%% -------------------------------------------------------------------

-module(deploy_track_s3).
-author("hazen").

-behaviour(gen_server).
-compile([{parse_transform, lager_transform}]).

%% API
-export([
    checkpoint/1, checkpoint/2,
    fetch_all_indexes/0, fetch_all_indexes/1,
    fetch_all_logs/1,
    fetch_checkpoint/0, fetch_checkpoint/1,
    fetch_indexes/0, fetch_indexes/1,
    fetch_logs/1,
    loop/0,
    parse_log_records/3,
    start_link/0,
    start_loop/0,
    stop_loop/0,
    write_analytics/1,
    write_analytics_record/1,
    write_csv/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export_type([aws_config/0]).

-define(SERVER, ?MODULE).
-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include("deploy_track.hrl").

-record(state, {
    s3_hostname    :: string(),
    products       :: [string()],
    dl_bucket      :: string(), %% Name of S3 download bucket
    dl_prefix = [] :: string(), %% Name of S3 download bucket prefix
    dl_config      :: aws_config(),
    up_bucket      :: string(), %% Name of S3 upload bucket (last recorded)
    up_key         :: string(), %% Name of S3 upload bucket key
    up_config      :: aws_config(),
    loop_timer     :: timer:tref() | undefined, %% Timer to periodically run main loop
    interval       :: integer() %% Milliseconds between loop calls
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({global, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Retrieve a list of download log entries from S3.
%% Result is {MoreResults, EndMarker, [Logs]}
%% @end
%%--------------------------------------------------------------------

-spec fetch_indexes() -> {boolean(), string(), list(string())}.
fetch_indexes() ->
    gen_server:call({global, ?SERVER}, {fetch_indexes, []}).

-spec fetch_indexes(Marker :: string()) -> {boolean(), string(), list()}.
fetch_indexes(Marker) ->
    gen_server:call({global, ?SERVER}, {fetch_indexes, Marker}).

%%--------------------------------------------------------------------
%% @doc
%% For a given Marker, return the corresponding log file entries, parsed.
%% Result is {MoreResults, EndMarker, [Logs]}
%% @end
%%--------------------------------------------------------------------

-spec fetch_logs(Marker :: string()) -> [#analytics{}].
fetch_logs(Marker) ->
    gen_server:call({global, ?SERVER}, {fetch_logs, Marker}, get_dl_timeout()).

%%--------------------------------------------------------------------
%% @doc
%% Retrieve the list of the following from S3 logs since Marker
%%    - Download Date/Time
%%    - Package
%%    - Version
%%    - Platform
%%    - IP
%% @end
%%--------------------------------------------------------------------

-spec fetch_all_logs(Marker :: string()) -> {string, [#analytics{}]}.
fetch_all_logs(Marker) ->
    gen_server:call({global, ?SERVER}, {fetch_all_logs, Marker}, get_dl_timeout()).

%%--------------------------------------------------------------------
%% @doc
%% Request all outstanding indexes since Marker. Return the last Marker
%% and the complete list of keys to log files.
%% @end
%%--------------------------------------------------------------------

-spec fetch_all_indexes() -> {string, [string()]}.
fetch_all_indexes() ->
    gen_server:call({global, ?SERVER}, {fetch_all_indexes, []}, get_dl_timeout()).

-spec fetch_all_indexes(Marker :: string()) -> {string, [string()]}.
fetch_all_indexes(Marker) ->
    gen_server:call({global, ?SERVER}, {fetch_all_indexes, Marker}, get_dl_timeout()).

%%--------------------------------------------------------------------
%% @doc
%% Create a CSV file of the log files since Marker
%% @end
%%--------------------------------------------------------------------

-spec write_csv(File :: string(), Marker :: string()) -> string().
write_csv(File, Marker) ->
    {ok, FP} = file:open(File, [write]),
    io:format(FP, "~s~n", [deploy_track_analytics:csv_header()]),
    write_csv(FP, Marker, true).

-spec write_csv(FP :: file:io_device(),
                Marker :: string(),
                MoreResults :: boolean()) -> string().
write_csv(FP, Marker, false) ->
    file:close(FP),
    Marker;
write_csv(FP, Marker, true) ->
    {MoreResults, EndMarker, Indexes} = fetch_indexes(Marker),
    lists:map(fun(Index) -> write_csv_record(FP, Index) end, Indexes),
    write_csv(FP, EndMarker, MoreResults).

-spec write_csv_record(FP :: file:io_device(),
                       Key :: string()) -> ok.
write_csv_record(FP, Key) ->
    Logs = fetch_logs(Key),
    lists:map(fun(Log) ->
        case Log of
            skip -> ok;
            _ -> io:format(FP, "~s~n",
                           [deploy_track_analytics:csv_format(Log)])
        end end, Logs),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Write all records to Google Analytics
%% @end
%%--------------------------------------------------------------------

-spec write_analytics(Marker :: string()) -> string().
write_analytics(Marker) ->
    gen_server:call({global, ?SERVER}, {write_analytics, Marker}).

-spec write_analytics_record(Marker :: string()) -> ok.
write_analytics_record(Marker) ->
    gen_server:call({global, ?SERVER}, {write_analytics_record, Marker}).

%%--------------------------------------------------------------------
%% @doc
%% Write a marker to S3 to mark the last successfully completed entry.
%% This is used by both S3 and PackageCloud services to checkpoint
%% progress.
%%
%% @end
%%--------------------------------------------------------------------
-spec checkpoint(Checkpoint :: string()) ->
                 ok | {error, Reason :: term()}.
checkpoint(Checkpoint) ->
    gen_server:call({global, ?SERVER}, {checkpoint, Checkpoint}, get_dl_timeout()).

-spec checkpoint(Key :: string(),
                 Checkpoint :: string()) ->
                    ok | {error, Reason :: term()}.
checkpoint(Key, Checkpoint) ->
    gen_server:call({global, ?SERVER}, {checkpoint, Key, Checkpoint}, get_dl_timeout()).

%%--------------------------------------------------------------------
%% @doc
%% Read a marker from S3 which marked the last successfully completed entry.
%%
%% @end
%%--------------------------------------------------------------------
-spec fetch_checkpoint() -> binary() | {error, Reason :: term()}.
fetch_checkpoint() ->
    gen_server:call({global, ?SERVER}, fetch_checkpoint, get_dl_timeout()).

-spec fetch_checkpoint(Key :: string()) -> binary() | {error, Reason :: term()}.
fetch_checkpoint(Key) ->
    gen_server:call({global, ?SERVER}, {fetch_checkpoint, Key}, get_dl_timeout()).


%%--------------------------------------------------------------------
%% @doc
%% The main processing loop.  It pulls the last successful key and
%% reads any changes since then and uploads to Google Analytics
%%
%% @end
%%--------------------------------------------------------------------

-spec loop() -> ok.
loop() ->
    gen_server:call({global, ?SERVER}, loop, get_dl_timeout()).

-spec start_loop() -> ok.
start_loop() ->
    gen_server:call({global, ?SERVER}, start_loop).

-spec stop_loop() -> ok.
stop_loop() ->
    gen_server:call({global, ?SERVER}, stop_loop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the S3 server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([]) ->
    ok = deploy_track_util:ensure_ssl_is_up(),
    ok = deploy_track_util:ensure_lager_is_up(),
    ok = deploy_track_util:ensure_services_are_up([xmerl, inets, jsx, lhttpc, erlcloud]),

    lager:info("Starting up ~p", [?MODULE]),
    {ok, S3Host} = application:get_env(deploy_track, s3_hostname),
    {ok, Products} = application:get_env(deploy_track, s3_products),
    {ok, DownloadBucket} = application:get_env(deploy_track, s3_dl_bucket),
    {ok, DownloadPrefix} = application:get_env(deploy_track, s3_dl_prefix),
    {ok, DownloadAccessKey} = application:get_env(deploy_track, s3_dl_access_key),
    {ok, DownloadSecretKey} = application:get_env(deploy_track, s3_dl_secret_key),
    DownloadConfig = make_aws_config(DownloadAccessKey, DownloadSecretKey, S3Host),
    {ok, UploadBucket} = application:get_env(deploy_track, s3_up_bucket),
    {ok, UploadKey} = application:get_env(deploy_track, s3_up_key),
    {ok, UploadAccessKey} = application:get_env(deploy_track, s3_up_access_key),
    {ok, UploadSecretKey} = application:get_env(deploy_track, s3_up_secret_key),
    UploadConfig = make_aws_config(UploadAccessKey, UploadSecretKey, S3Host),
    {ok, Interval} = application:get_env(deploy_track, s3_interval),
    {ok, #state{
                s3_hostname     = S3Host,
                products        = Products,
                dl_bucket       = DownloadBucket,
                dl_prefix       = DownloadPrefix,
                dl_config       = DownloadConfig,
                up_bucket       = UploadBucket,
                up_key          = UploadKey,
                up_config       = UploadConfig,
                loop_timer      = undefined,
                interval        = Interval
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call({fetch_indexes, Marker}, _From, State) ->
    Result = do_fetch_indexes(Marker, State),
    {reply, Result, State};
handle_call({fetch_all_indexes, Marker}, _From, State) ->
    Result = do_fetch_all_indexes(Marker, State),
    {reply, Result, State, get_dl_timeout()};
handle_call({fetch_logs, Index}, _From, State ) ->
    Result = do_fetch_logs(Index, State),
    {reply, Result, State};
handle_call({fetch_all_logs, Marker}, _From, State) ->
    Parsed = do_fetch_all_logs(Marker, State),
    {reply, Parsed, State, get_dl_timeout()};
handle_call({checkpoint, Checkpoint}, _From,
            State = #state{up_key = Key}) ->
    Result = do_write_checkpoint(Key, Checkpoint, State),
    {reply, Result, State};
handle_call({checkpoint, Key, Checkpoint}, _From, State) ->
    Result = do_write_checkpoint(Key, Checkpoint, State),
    {reply, Result, State};
handle_call(fetch_checkpoint, _From, State = #state{up_key = Key}) ->
    Result = do_fetch_checkpoint(Key, State),
    {reply, Result, State};
handle_call({fetch_checkpoint, Key}, _From, State) ->
    Result = do_fetch_checkpoint(Key, State),
    {reply, Result, State};
handle_call({write_analytics, Marker}, _From, State) ->
    Result = do_write_analytics(Marker, State),
    {reply, Result, State};
handle_call({write_analytics_record, Marker}, _From, State) ->
    Result = do_write_analytics_record(Marker, State),
    {reply, Result, State};
handle_call(loop, _From, State = #state{loop_timer = Timer,
                                        interval   = Interval}) ->
    Result = do_loop(State),
    %% If there is no current timer, don't start a new one
    NewTimer = case Timer of
        undefined ->
            Timer;
        _ ->
            deploy_track_util:cancel_current_timer(Timer),
            {ok, T} = schedule_loop(Interval),
            T
    end,
    {reply, Result, State#state{loop_timer = NewTimer}};
handle_call(start_loop, _From, State = #state{loop_timer = Timer}) ->
    deploy_track_util:cancel_current_timer(Timer),
    %% If we are restarting a loop, fire it off after 1 sec
    {ok, NewTimer} = schedule_loop(1000),
    {reply, ok, State#state{loop_timer = NewTimer}};
handle_call(stop_loop, _From, State = #state{loop_timer = Timer}) ->
    deploy_track_util:cancel_current_timer(Timer),
    {reply, ok, State#state{loop_timer = undefined}};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert a single logfile string into a Google Analytics record
%% @end
%%--------------------------------------------------------------------

-spec parse_log_records(Key :: string(),
                        Record :: deploy_track_util:proplist(),
                        Products :: [string()]) ->
                        [skip | #analytics{}].
parse_log_records(Key, Record, Products) ->
    Content = binary_to_list(proplists:get_value(content, Record)),
    Entries = string:tokens(Content, "\n"),
    lists:map(fun(X) -> parse_log_record(Key, X, Products) end, Entries).

-spec parse_log_record(Key :: string(),
                       Entry :: string(),
                       Products :: [string()]) -> skip | #analytics{}.
parse_log_record(Key, Entry, Products) ->
    Elements = [Key] ++ deploy_track_util:split_with_quotes(Entry),
    lager:debug("Elements ~p", [Elements]),
    case skip_invalid_records(Elements, Products) of
        skip ->
            lager:debug("Skipping ~p", [Elements]),
            skip;
        _ -> build_analytics(Elements)
    end.

-spec skip_invalid_records(Elements :: [string()],
                           Products :: [string()]) ->
                           skip | #analytics{}.
skip_invalid_records(Elements, Products) ->
    Elements1 = skip_non_get_requests(Elements),
    Elements2 = skip_invalid_filename(Elements1),
    Elements3 = skip_doc_files(Elements2),
    skip_uninteresting_projects(Elements3, Products).

%% Only record GET requests (lots of others are in S3)
-spec skip_non_get_requests(Elements :: [string()]) ->
                            skip | [string()].
skip_non_get_requests(Elements) ->
    HTTPMethod = get_http_method(Elements),
    case HTTPMethod of
        "REST.GET.OBJECT" -> Elements;
        _ -> skip
    end.

%% Skip downloads which cannot be parsed, e.g., Erlang packages
-spec skip_invalid_filename(Elements :: skip | [string()]) ->
                            skip | [string()].
skip_invalid_filename(skip) ->
    skip;
skip_invalid_filename(Elements) ->
    case parse_prod_version(Elements) of
        nomatch -> skip;
        _ -> Elements
    end.

%% Requests for documentation are ignored
-spec skip_doc_files(Elements :: skip | [string()]) ->
                     skip | [string()].
skip_doc_files(skip) ->
    skip;
skip_doc_files(Elements) ->
    Filename = get_filename(Elements),
    IsDocFile = string:str(Filename, "index"),
    case IsDocFile of
        0 -> Elements;
        _ -> skip
    end.

%% If this product is not in the list of interesting products
%% just skip it
-spec skip_uninteresting_projects(Elements :: skip | [string()],
                                  Products :: [string()]) ->
                                  skip | [string()].
skip_uninteresting_projects(skip, _Products) ->
    skip;
skip_uninteresting_projects(Elements, Products) ->
    {match, [Product, _Version]} = parse_prod_version(Elements),
    case ((Products =:= []) or lists:member(Product, Products)) of
        true -> Elements;
        _ -> skip
    end.

%% This is a valid log. Convert to an analytics record.
-spec build_analytics(Elements :: [string()]) -> #analytics{}.
build_analytics(Elements) ->
    Filename = get_filename(Elements),
    {match, [Product, Version]} = parse_prod_version(Elements),
    ProductVersionOS =
        re:run(Filename, "^([^/]+)/[^/]+/([^/]+)/([^/]+)/([^/]+)",
            [{capture, all_but_first, list}]),
    %% It seems that CS/S2 does not have platform versions
    [OS, OSVersion] = case ProductVersionOS of
                          nomatch ->
                              ["-","-"];
                          {match, [_Prod, _Ver, OName, OVersion]} ->
                              [OName, OVersion]
                      end,
    Key = lists:nth(1, Elements),
    Timestamp = lists:nth(4, Elements),
    {match, [Day, Month, Year, Hour, Minute, Second]} =
        re:run(Timestamp, "(\\d+)/([^/]+)/(\\d+):(\\d+):(\\d+):(\\d+)",
            [{capture, all_but_first, list}]),
    MonNum = deploy_track_util:mon_to_num(Month),
    IP = lists:nth(6, Elements),
    UserAgent = lists:nth(19, Elements),
    #analytics{
        key = Key,
        source = s3,
        timestamp = {{list_to_integer(Year),
            MonNum,
            list_to_integer(Day)},
            {list_to_integer(Hour),
                list_to_integer(Minute),
                list_to_integer(Second)}},
        ip = IP,
        filename = Filename,
        product = Product,
        version = Version,
        os = OS,
        os_version = OSVersion,
        user_agent = UserAgent}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_http_method(Elements :: [string()]) ->
                      string().
get_http_method(Elements) ->
    lists:nth(9, Elements).

-spec get_filename(Elements :: [string()]) ->
                   string().
get_filename(Elements) ->
    lists:nth(10, Elements).

%% From the filename, determine the product and version
-spec parse_prod_version(Elements :: [string()]) ->
    nomatch | {match, [string()]}.
parse_prod_version(Elements) ->
    Filename = get_filename(Elements),
    re:run(Filename, "^([^/]+)/[^/]+/([^/]+)",
           [{capture, all_but_first, list}]).

-spec get_dl_timeout() -> integer().
get_dl_timeout() ->
    application:get_env(deploy_track, s3_dl_timeout, 600000).

-spec make_aws_config(AccessKey :: string(),
                      SecretKey :: string(),
                      Host :: string()) ->
      #aws_config{}.
make_aws_config(AccessKey, SecretKey, _Host) ->
    Cfg = erlcloud_s3:new(AccessKey, SecretKey),
    Cfg#aws_config{retry = fun erlcloud_retry:default_retry/1}.

%% @doc Determine if the S3 option "prefix" is present
-spec has_prefix(list()) -> [] | list(tuple()).
has_prefix([]) ->
    [];
has_prefix(Prefix) ->
    [{prefix, Prefix}].

-spec do_fetch_all_indexes(Marker :: string(),
                           State :: #state{})  ->
                              {string, [string()]}.
do_fetch_all_indexes(Marker, State) ->
    fetch_all_indexes(Marker, State, [], true).

-spec fetch_all_indexes(Marker :: string(),
                        State :: #state{},
                        Acc :: [string()],
                        MoreResults :: boolean()) -> {string, [string()]}.
fetch_all_indexes(Marker, _State, Acc, false) ->
    {Marker, Acc};
fetch_all_indexes(Marker, State, Acc, _MoreResults) ->
    %% Fetch the next set of indexes
    {MoreResults, EndMarker, Indexes} = do_fetch_indexes(Marker, State),
    fetch_all_indexes(EndMarker, State, Acc ++ Indexes, MoreResults).

%% Fetch all of the actual log files starting at Marker
-spec do_fetch_all_logs(Marker :: string(),
                        State :: #state{})  ->
                           {string, [string()]}.
do_fetch_all_logs(Marker, State) ->
    do_fetch_all_logs(Marker, State, [], true).

-spec do_fetch_all_logs(Marker :: string(),
                        State :: #state{},
                        Acc :: [string()],
                        MoreResults :: boolean()) -> {string, [#analytics{}]}.
do_fetch_all_logs(Marker, _State, Acc, false) ->
    {Marker, Acc};
do_fetch_all_logs(Marker, State, Acc, _MoreResults) ->
    {MoreResults, EndMarker, Indexes} = do_fetch_indexes(Marker, State),
    Logs = lists:flatten(lists:map(fun(Idx) ->
                                       do_fetch_logs(Idx, State)
                                   end, Indexes)),
    FilteredLogs =
        lists:filter(fun(X) -> case X of skip -> false; _ -> true end end,
            Logs),
    do_fetch_all_logs(EndMarker, State, Acc ++ FilteredLogs, MoreResults).

-spec do_loop(#state{}) -> string().
do_loop(State = #state{up_key = Key}) ->
    lager:info("Starting S3 loop"),
    %% Start where we left off
    Checkpoint = do_fetch_checkpoint(Key, State),
    do_write_analytics(Checkpoint, State).

%% Read a list of indexes starting from Checkpoint
-spec do_fetch_indexes(Marker :: string(),
                       State :: #state{}) -> {boolean(), string(), [string()]}.
do_fetch_indexes(Marker,
                 #state{dl_bucket = Bucket,
                        dl_prefix = Prefix,
                        dl_config = Cfg}) ->
    Options = [{marker, Marker}] ++ has_prefix(Prefix),
    Response = erlcloud_s3:list_objects(Bucket, Options, Cfg),
    Contents = proplists:get_value(contents, Response),
    Truncated = proplists:get_value(is_truncated, Response),
    Indexes = lists:map(fun(X) -> proplists:get_value(key, X) end, Contents),
    EndMarker = hd(lists:reverse(Indexes)),
    {Truncated, EndMarker, Indexes}.


-spec do_fetch_logs(Index :: string(),
                    State :: #state{}) -> [skip | #analytics{}].
do_fetch_logs(Index,
              #state{dl_bucket = Bucket,
                     dl_config = Cfg,
                     products = Products}) ->
    Record = erlcloud_s3:get_object(Bucket, Index, Cfg),
    parse_log_records(Index, Record, Products).

%% Write the last successful key to S3
-spec do_write_checkpoint(Key :: string(),
                          Checkpoint :: string(),
                          State :: #state{}) -> [{version_id, string()}].
do_write_checkpoint(Key, Checkpoint,
                    #state{up_bucket = Bucket,
                           up_config = Cfg}) ->
    Result = erlcloud_s3:put_object(Bucket, Key, Checkpoint, Cfg),
    lager:info("Wrote checkpoint ~s to ~s/~s resulting in ~p",
               [Checkpoint, Bucket, Key, Result]),
    Result.

%% Read the last successful key to S3
-spec do_fetch_checkpoint(Key :: string(),
                          State :: #state{})-> string().
do_fetch_checkpoint(Key,
                    #state{up_bucket = Bucket,
                           up_config = Cfg}) ->
    lager:debug("Reading checkpoint from ~s/~s",
                [Bucket, Key]),
    Result = erlcloud_s3:get_object(Bucket, Key, Cfg),
    Checkpoint = case proplists:get_value(content, Result) of
                     undefined ->
                         "Checkpoint Undefined";
                     Binary ->
                         binary_to_list(Binary)
                 end,
    lager:info("Read checkpoint ~p from ~s/~s",
               [Checkpoint, Bucket, Key]),
    Checkpoint.

%% Write all of the logs at a given index to Google Analytics
-spec do_write_analytics_record(Index :: string(),
                                State :: #state{}) -> ok.
do_write_analytics_record(Index, State) ->
    Logs = do_fetch_logs(Index, State),
    FilteredLogs =
        lists:filter(fun(X) ->
            case X of
                skip ->
                    false;
                _ ->
                    true
            end end,
            Logs),
    lists:map(
        fun(Log) ->
            deploy_track_analytics:write_record(Log)
        end, FilteredLogs),
    ok.

%% Do all of checkpoint validation, writing to GA and updating the checkpoint
-spec do_write_analytics(Marker :: string(),
                         State :: #state{}) -> string() | {error, term()}.
do_write_analytics(Marker, State) ->
    do_write_analytics(Marker, State, true).

-spec do_write_analytics(Marker :: string(),
                         State :: #state{},
                         MoreResults :: boolean()) ->
                         string() | {error, term()}.
do_write_analytics(Marker, _State, false) ->
    Marker;
do_write_analytics(Marker, State, true) ->
    {MoreResults, EndMarker, Indexes} = do_fetch_indexes(Marker,
                                                         State),
    case write_each_index(Marker, Indexes, State) of
        {_, true} ->
            do_write_analytics(EndMarker, State, MoreResults);
        _ ->
            {error, checkpoint_mismatch}
    end.


%% Write all of the indexes in this batch as long as the state
%% is still valid, i.e., no one else has moved the checkpoint
-spec write_each_index(Checkpoint :: string(),
                       Indexes :: [string()],
                       State :: #state{}) -> {string(), boolean()}.
write_each_index(Checkpoint, Indexes, State = #state{up_key = Key}) ->
    lists:foldl(
        fun(_, {Marker, false}) ->
            {Marker, false};
        (Idx, {Marker, _Valid}) ->
            case verify_checkpoint(Marker, State) of
                true ->
                    do_write_analytics_record(Idx, State),
                    do_write_checkpoint(Key, Idx, State),
                    {Idx, true};
                _ ->
                    {Marker, false}
            end
        end, {Checkpoint, true}, Indexes).

-spec verify_checkpoint(Marker :: string(),
                        State :: #state{}) -> boolean().
verify_checkpoint(Marker, State = #state{up_key = Key}) ->
    Checkpoint = do_fetch_checkpoint(Key, State),
    Valid = (Checkpoint == Marker),
    case Valid of
        false ->
            lager:error("The checkpoint was ~p and now is ~p",
                        [Marker, Checkpoint]);
        _ ->
            ok
    end,
    Valid.

%% Schedule the next loop
-spec schedule_loop(Interval :: integer()) -> {ok, timer:tref()} | {error, term()}.
schedule_loop(Interval) ->
    timer:apply_after(Interval, global:whereis_name(?SERVER), do_loop, []).



