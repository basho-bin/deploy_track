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
%% from PackageCloud.io.
%%
%% Details at https://packagecloud.io/docs/api
%%
%% -------------------------------------------------------------------

-module(deploy_track_pkgcloud).
-author("hazen").

-behaviour(gen_server).
-compile([{parse_transform, lager_transform}]).

%% API
-export([
    build_analytics/1,
    build_key/1,
    cache_packages/0,
    checkpoint/1,
    fetch_all_packages/0,
    fetch_checkpoint/0,
    fetch_details/2,
    fetch_packages/1,
    fetch_repos/0,
    filter_out_logged_events/2,
    fetch_next_events/3,
    loop/0,
    sort_details/2,
    start_link/0,
    start_loop/0,
    stop_loop/0,
    do_loop/1
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

-define(SERVER, ?MODULE).
-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include("deploy_track.hrl").

-record(state, {
    base_url   :: string(),
    params     :: deploy_track_util:proplist(),
    products   :: [string()],
    packages   :: [deploy_track_util:proplist()],
    up_bucket  :: string(), %% Name of S3 upload bucket (last recorded)
    up_key     :: string(), %% Name of S3 upload bucket key
    up_config  :: aws_config(), %% S3 upload credentials
    loop_timer :: timer:tref() | undefined, %% Timer to periodically run main loop
    interval   :: integer() %% Milliseconds between loop calls
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
%% Retrieve a list of repos from PackageCloud
%% @end
%%--------------------------------------------------------------------

-spec fetch_repos() -> list().
fetch_repos() ->
    gen_server:call({global, ?SERVER}, repos).

%%--------------------------------------------------------------------
%% @doc
%% Retrieve a list of packages for a specific repo from PackageCloud
%% @end
%%--------------------------------------------------------------------

-spec fetch_packages(Repo :: string()) -> list().
fetch_packages(Repo) ->
    gen_server:call({global, ?SERVER}, {packages, Repo}).

%%--------------------------------------------------------------------
%% @doc
%% Retrieve a list of packages for all repos from PackageCloud
%% @end
%%--------------------------------------------------------------------

-spec fetch_all_packages() -> [deploy_track_util:proplist()].
fetch_all_packages() ->
    gen_server:call({global, ?SERVER}, all_packages).

%%--------------------------------------------------------------------
%% @doc
%% Store a list of all desired packages in the gen_server state
%% @end
%%--------------------------------------------------------------------

-spec cache_packages() -> ok.
cache_packages() ->
    gen_server:call({global, ?SERVER}, cache_packages).

%%--------------------------------------------------------------------
%% @doc
%% Retrieve a the download details of a particular package
%% @end
%%--------------------------------------------------------------------

-spec fetch_details(Package :: deploy_track_util:proplist(),
                    Arguments :: deploy_track_util:proplist()) ->
    [deploy_track_util:proplist()].
fetch_details(Package, Arguments) ->
    gen_server:call({global, ?SERVER}, {detail, Package, Arguments}).

%%--------------------------------------------------------------------
%% @doc
%% Write a marker to S3 to mark the last successfully completed entry.
%%
%% @end
%%--------------------------------------------------------------------
-spec checkpoint(Checkpoint :: string()) ->
    ok | {error, Reason :: term()}.
checkpoint(Checkpoint) ->
    gen_server:call({global, ?SERVER}, {checkpoint, Checkpoint}, get_pkgcloud_timeout()).

%%--------------------------------------------------------------------
%% @doc
%% Read a marker from S3 which marked the last successfully completed entry.
%%
%% @end
%%--------------------------------------------------------------------
-spec fetch_checkpoint() -> binary() | {error, Reason :: term()}.
fetch_checkpoint() ->
    gen_server:call({global, ?SERVER}, fetch_checkpoint, get_pkgcloud_timeout()).

%%--------------------------------------------------------------------
%% @doc
%% The main processing loop.  It pulls the last successful key and
%% reads any changes since then and uploads to Google Analytics
%%
%% @end
%%--------------------------------------------------------------------

-spec loop() -> ok.
loop() ->
    gen_server:call({global, ?SERVER}, loop, get_pkgcloud_timeout()).

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
%% Initializes the server
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
    ok = deploy_track_util:ensure_services_are_up([ibrowse]),

    lager:info("Starting up ~p", [?MODULE]),
    {ok, Products} = application:get_env(deploy_track, pkgcloud_products),
    {ok, ApiKey} = application:get_env(deploy_track, pkgcloud_key),
    {ok, User} = application:get_env(deploy_track, pkgcloud_user),
    {ok, Interval} = application:get_env(deploy_track, s3_interval),
    {ok, UploadBucket} = application:get_env(deploy_track, s3_up_bucket),
    {ok, UploadKey} = application:get_env(deploy_track, pkgcloud_up_key),
    {ok, UploadAccessKey} = application:get_env(deploy_track, s3_up_access_key),
    {ok, UploadSecretKey} = application:get_env(deploy_track, s3_up_secret_key),
    {ok, S3Host} = application:get_env(deploy_track, s3_hostname),
    UploadConfig = deploy_track_s3:make_aws_config(UploadAccessKey, UploadSecretKey, S3Host),
    %% Start the main loop more or less immediately
    schedule_loop(1000),
    {ok, #state{base_url   = build_base_url(ApiKey),
                params     = [{user, User}],
                products   = Products,
                packages   = [],
                up_bucket  = UploadBucket,
                up_key     = UploadKey,
                up_config  = UploadConfig,
                loop_timer = undefined,
                interval   = Interval}}.

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
handle_call(repos, _From, State) ->
    Reply = do_repos(State),
    {reply, Reply, State};
handle_call({packages, Repo}, _From, State) ->
    Reply = do_packages(Repo, State),
    {reply, Reply, State};
handle_call(all_packages, _From, State) ->
    Reply = do_all_packages(State),
    {reply, Reply, State};
handle_call(cache_packages, _From, State) ->
    State1 = do_cache_packages(State),
    {reply, ok, State1};
handle_call({detail, Package, Args}, _From, State) ->
    Reply = do_details(Package, Args, State),
    {reply, Reply, State};
handle_call({count, Params}, _From, State) ->
    do_count(Params, State),
    Reply = do_count(Params, State),
    {reply, Reply, State};
handle_call({checkpoint, Checkpoint}, _From,
    State = #state{up_key = Key}) ->
    Result = do_write_checkpoint(Key, Checkpoint, State),
    {reply, Result, State};
handle_call(fetch_checkpoint, _From, State = #state{up_key = Key}) ->
    Result = do_fetch_checkpoint(Key, State),
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
terminate(Reason, _State) ->
    lager:info("Stopping ~p for reason ~p", [?MODULE, Reason]),
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_pkgcloud_timeout() -> integer().
get_pkgcloud_timeout() ->
    application:get_env(deploy_track, pkgcloud_timeout, 600000).

-spec build_base_url(ApiKey :: binary()) -> string().
build_base_url(ApiKey) ->
    deploy_track_util:flat_format("https://~s:@packagecloud.io", [ApiKey]).

%% @doc Send an HTTP request to the server
-spec send_request(State :: #state{}, Method :: atom(),
    ReqParams :: deploy_track_util:proplist(), PathTemplate :: string(),
    Payload :: deploy_track_util:proplist()) ->
    {ok, string()} | {error, {string(), integer(), list(), string()}} | term().
send_request(#state{base_url = BaseUrl, params = Params},
             Method, ReqParams, PathTemplate, Payload) ->
    deploy_track_util:send_request(BaseUrl, Method, ReqParams ++ Params,
                                   PathTemplate, Payload).

-spec do_repos(State :: #state{}) -> [deploy_track_util:proplist()].
do_repos(State) ->
    %% GET /api/v1/repos
    Template = "/api/v1/repos",
    {ok, Reply} = deploy_track_util:ignore_not_found(send_request(State, get, [], Template, [])),
    Reply.

-spec do_packages(Repos :: [string()], State :: #state{}) -> [deploy_track_util:proplist()].
%% For an empty list, fetch all of the packages
do_packages([], State) ->
    do_all_packages(State);
do_packages(Repos, State) ->
    %% GET /api/v1/repos/:user/:repo/packages.json
    Template = "/api/v1/repos/:user/:repo/packages.json",
    lists:foldr(fun(Repo, Acc) ->
        Params = [{repo, Repo}],
        {ok, Reply} = deploy_track_util:ignore_not_found(send_request(State, get, Params, Template, [])),
        lists:append(Acc, Reply)
        end, [], Repos).

-spec do_all_packages(State :: #state{}) -> [[deploy_track_util:proplist()]].
do_all_packages(State) ->
    Repos = [proplists:get_value(name,X) || X <- do_repos(State)],
    do_packages(Repos, State).

-spec do_cache_packages(State :: #state{}) -> #state{}.
do_cache_packages(State = #state{packages = Pkgs,
                                 products = Products}) ->
    Packages = case Pkgs of
        [] -> do_packages(Products, State);
        _ -> Pkgs
    end,
    State#state{packages = Packages}.

-spec do_details(string(), deploy_track_util:proplist(), #state{}) -> [deploy_track_util:proplist()].
do_details(Package, Args, State) ->
    %% GET /api/v1/repos/:user/:repo/package/:type/:distro/:version/:package/:arch/:package_version-:release/stats/downloads/detail.json
    Url = binary_to_list(proplists:get_value(package_url, Package)),
    %% Trim off the trailing ".json" and add the correct suffix
    Template = string:left(Url, length(Url)-5) ++ "/stats/downloads/detail.json",
    QueryString = [{per_page, "100000"}] ++ Args,
    {ok, Detail} = deploy_track_util:ignore_not_found(send_request(State, get, [], Template, QueryString)),
    [lists:append(Package, D) || D <- Detail].

-spec do_count(Params :: deploy_track_util:proplist(), State :: #state{}) -> integer().
do_count(Params, State) ->
    %% GET /api/v1/repos/:user/:repo/package/:type/:distro/:version/:package/:arch/:package_version-:release/stats/downloads/count+.json
    Template = "//api/v1/repos/:user/:repo/package/:type/:distro/:version/:package/:arch/:package_version-:release/stats/downloads/count.json",
    {ok, Reply} = deploy_track_util:ignore_not_found(send_request(State, get, Params, Template, [])),
    Reply.

-spec do_loop(State :: #state{}) -> string().
do_loop(State = #state{products = Products,
                       up_key = Key}) ->
    Marker = do_fetch_checkpoint(Key, State),
    lager:debug("Looking for these products ~p", [Products]),
    Pkgs = do_packages(Products, State),
    lager:debug("Loaded ~b packages", [length(Pkgs)]),
    Details = fetch_next_events(Pkgs, Marker, State),
    lager:debug("Fetch a total of ~b events", [length(Details)]),
    Filtered = filter_out_logged_events(Details, Marker),
    lager:debug("Ready to write ~b events", [length(Filtered)]),
    Analytics = [build_analytics(X) || X <- Filtered],
    do_write_analytics(Analytics, Marker, State).

%% Pull back all events since the Checkpoint date
-spec fetch_next_events(Pkgs :: [deploy_track_util:proplist()],
                        Checkpoint :: string(),
                        State :: #state{}) ->
    [deploy_track_util:proplist()].
fetch_next_events(Pkgs, Checkpoint, State) ->
    {match, [Year, Month, Day]} =
        re:run(Checkpoint, "^(\\d+)-(\\d+)-(\\d+)",
        [{capture, all_but_first, list}]),
    StartDate = deploy_track_util:flat_format("~s~s~sZ",[Year, Month, Day]),
    lager:info("Retrieving events from date ~p", [StartDate]),
    Details = lists:foldr(
        fun(P, Acc) ->
            lager:debug("Fetching events from package ~p", [P]),
            Details = do_details(P, [{start_date, StartDate}], State),
            lager:debug("Found events: ~p", [Details]),
            lager:debug("Found ~b events", [length(Details)]),
            lists:append(Acc, Details)
        end, [], Pkgs),
    lists:sort(fun sort_details/2, Details).

%% The last successfully logged event is the Checkpoint.  Remove
%% all of the events in the day's list which happened before the
%% Checkpoint event.
-spec filter_out_logged_events(Details :: [deploy_track_util:proplist()],
                               Checkpoint :: string()) ->
                               [deploy_track_util:proplist()].
filter_out_logged_events(Details, Checkpoint) ->
   Events = lists:dropwhile(
       fun(X) ->
            Key = build_key(X),
            Checkpoint /= Key
       end, Details),
    %% Skip the first one, because it matches our Checkpoint
    case Events of
        [_|T] -> T;
        _ -> []
    end.

%% For a list of analytics, write the values and update the Checkpoint
-spec do_write_analytics(Analytics :: [#analytics{}],
    Marker :: string(),
    State :: #state{}) -> string().
do_write_analytics([], Marker, _State) ->
    Marker;
do_write_analytics([H|T], Marker, State = #state{up_key = Key}) ->
    NewMarker = H#analytics.key,
    case verify_checkpoint(Marker, State) of
        true ->
            deploy_track_analytics:write_record(H),
            do_write_checkpoint(Key, NewMarker, State),
            do_write_analytics(T, NewMarker, State);
        _ ->
            lager:error("Checkpoint has been updated by another process."),
            Marker
    end.

-spec build_key(Details :: deploy_track_util:proplist()) -> string().
build_key(Details) ->
    Product = get_prop_as_list(name, Details),
    Version = get_prop_as_list(version, Details),
    Timestamp = get_prop_as_list(downloaded_at, Details),
    IP = get_prop_as_list(ip_address, Details),
    deploy_track_util:flat_format("~s ~s ~s ~s",
                                  [Timestamp, Product, Version, IP]).

-spec sort_details(Details1 :: deploy_track_util:proplist(),
                   Details2 :: deploy_track_util:proplist()) ->
                   boolean().
sort_details(Details1, Details2) ->
    Key1 = build_key(Details1),
    Key2 = build_key(Details2),
    Key1 < Key2.

-spec get_prop_as_list(Prop :: atom(),
                       Proplist :: deploy_track_util:proplist()) -> string().
get_prop_as_list(Prop, Proplist) ->
    binary_to_list(proplists:get_value(Prop, Proplist)).

%% Convert installation details into an analytics record
-spec build_analytics(Details :: deploy_track_util:proplist()) -> #analytics{}.
build_analytics(Details) ->
    Product = get_prop_as_list(name, Details),
    Version = get_prop_as_list(version, Details),
    DistroVersion = get_prop_as_list(distro_version, Details),
    [OS, OSVersion] = string:tokens(DistroVersion, "/"),
    Filename = get_prop_as_list(filename, Details),
    Release = get_prop_as_list(release, Details),
    Key = build_key(Details),
    %% {created_at,<<"2016-05-05T00:52:17.000Z">>},
    Timestamp = get_prop_as_list(downloaded_at, Details),
    {match, [Year, Month, Day, Hour, Minute, Second]} =
        re:run(Timestamp, "(\\d+)-(\\d+)-(\\d+)T(\\d+):(\\d+):(\\d+)",
            [{capture, all_but_first, list}]),
    IP = get_prop_as_list(ip_address, Details),
    UserAgent = get_prop_as_list(user_agent, Details),
    #analytics{
        key = Key,
        source = pkgcloud,
        timestamp = {{list_to_integer(Year),
            list_to_integer(Month),
            list_to_integer(Day)},
            {list_to_integer(Hour),
            list_to_integer(Minute),
            list_to_integer(Second)}},
        ip = IP,
        filename = Filename,
        product = Product,
        version = Version,
        release = Release,
        os = OS,
        os_version = OSVersion,
        user_agent = UserAgent}.

%% Schedule the next loop
-spec schedule_loop(Interval :: integer()) -> {ok, timer:tref()} | {error, term()}.
schedule_loop(Interval) ->
    lager:debug("Running ~p:do_loop after ~b ms", [?MODULE, Interval]),
    timer:apply_after(Interval, ?MODULE, loop, []).

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

%% Has someone moved my cheese? Did another process checkpoint?
%% It's more of a sanity check than a solid guarantee
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