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
%% This module is responsible for uploading download information
%% up to Google Analytics.
%%
%% https://developers.google.com/analytics/
%% https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters
%%
%% -------------------------------------------------------------------

-module(deploy_track_analytics).
-author("hazen").

-behaviour(gen_server).
-compile([{parse_transform, lager_transform}]).

%% API
-export([
    csv_format/1,
    csv_header/0,
    encode_payload/2,
    start_link/0,
    write_record/1
]).
-type source() :: pkgcloud | s3 | github.
-export_type([source/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(FOUR_HOURS, 4 * 60 * 60).
-include("deploy_track.hrl").

-record(state, {
    url        :: string(),
    tid        :: binary() %% tracking id
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the Google Analytics server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({global, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% CSV Header record
%% @end
%%--------------------------------------------------------------------

-spec csv_header() -> string().
csv_header() ->
    Fmt = csv_format_string(),
    deploy_track_util:flat_format(Fmt,
        ["Key",
         "Source",
         "Timestamp",
         "IP",
         "Filename",
         "Product",
         "Version",
         "Release",
         "OS",
         "OS Version",
         "User Agent"]).

%%--------------------------------------------------------------------
%% @doc
%% Format log entry as a CSV
%% @end
%%--------------------------------------------------------------------

-spec csv_format(A :: #analytics{}) -> string().
csv_format(A) ->
    Fmt = csv_format_string(),
    deploy_track_util:flat_format(Fmt,
        [A#analytics.key,
         atom_to_list(A#analytics.source),
         deploy_track_util:iso_8601_date_format(A#analytics.timestamp),
         A#analytics.ip,
         A#analytics.filename,
         A#analytics.product,
         A#analytics.version,
         A#analytics.release,
         A#analytics.os,
         A#analytics.os_version,
         A#analytics.user_agent]).

csv_format_string() ->
    string:join(lists:duplicate(11, "\"~s\""), ",") ++
       ",~s".

%%--------------------------------------------------------------------
%% @doc
%% Write a single Analytics record
%% @end
%%--------------------------------------------------------------------

-spec write_record(Record :: #analytics{}) -> ok.
write_record(Record) ->
    gen_server:call({global, ?SERVER}, {write_record, Record}).

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

    lager:info("Starting up ~p", [?MODULE]),
    Url = application:get_env(deploy_track, analytics_url,
                              "https://www.google-analytics.com/collect"),
    {ok, Tid} = application:get_env(deploy_track, analytics_tid),
    lager:info("Using GA TID ~p", [Tid]),
    {ok, #state{url = Url,
                tid = Tid}}.

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
handle_call({write_record, Record}, _From, State) ->
    Result = do_write_analytics_record(Record, State),
    {reply, Result, State};
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Prepare the GA hit payload
-spec encode_payload(A :: #analytics{}, #state{}) -> string().
encode_payload(A = #analytics{}, #state{tid = Tid}) ->
    IP = A#analytics.ip,
    Agent = A#analytics.user_agent,
    UuidKey = crypto:hash(sha, IP ++ Agent),
    Cid = uuid_string(UuidKey),
    ExtractTime = calendar:datetime_to_gregorian_seconds(A#analytics.timestamp),
    Now = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
    Delta = Now - ExtractTime,
    %% Google Analytics has a window of 4 hours, if we miss it then go
    %% with the max
    QueueTime = case Delta < ?FOUR_HOURS of
        true -> case Delta < 0 of
                    true -> 0;
                    _ -> Delta * 1000 %% Convert to milliseconds

                end;
        _ -> ?FOUR_HOURS * 1000 - 1
    end,
    Args = [{v, 1},
            {tid, Tid},
            {an, "Riak"},
            {t, event},
            {ec, deploy},
            {ea, A#analytics.source},
            {ds, A#analytics.source},
            {qt, QueueTime},
            {cid, Cid},
            {uip, IP},
            {cd1, A#analytics.product},
            {cd2, A#analytics.version},
            {cd3, A#analytics.os},
            {cd4, A#analytics.os_version},
            {cd5, deploy_track_util:iso_8601_date_format(A#analytics.timestamp)},
            {ua, A#analytics.user_agent}],
    deploy_track_util:encode_query_string(Args) ++ "\r\n".

-spec uuid_string(binary()) -> string().
uuid_string(U) ->
    deploy_track_util:flat_format("~8.16.0b-~4.16.0b-~4.16.0b-~2.16.0b~2.16.0b-~12.16.0b",
                                  get_uuid_parts(U)).

%% UUID defined: https://tools.ietf.org/html/rfc4122
%% UUID                   = time-low "-" time-mid "-"
%%                          time-high-and-version "-"
%%                          clock-seq-and-reserved
%%                          clock-seq-low "-" node
%% time-low               = 4hexOctet
%% time-mid               = 2hexOctet
%% time-high-and-version  = 2hexOctet
%% clock-seq-and-reserved = hexOctet
%% clock-seq-low          = hexOctet
%% node                   = 6hexOctet
-spec get_uuid_parts(binary()) -> [integer()].
get_uuid_parts(<<TL:32, TM:16, THV:16, CSR:8, CSL:8, N:48, _/binary>>) ->
    [TL, TM, THV, CSR, CSL, N].

%% Actually send the analytics record to GA
-spec do_write_analytics_record(Record :: #analytics{},
                                State :: #state{}) -> term().
do_write_analytics_record(Record, State = #state{url = Url}) ->
    Payload = encode_payload(Record, State),
    lager:debug("Posting to GA ~p", [Payload]),
    Result = deploy_track_util:send_request(Url, post, [], [], [], Payload),
    lager:debug("GA Result = ~p", [Result]),
    {ok, _} = Result,
    Result.
