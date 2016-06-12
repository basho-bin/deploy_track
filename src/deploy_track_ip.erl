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
%% This module is responsible looking up information for a
%% given IP.
%%
%% http://ip-api.com/docs
%%
%% -------------------------------------------------------------------

-module(deploy_track_ip).
-author("hazen").

-behaviour(gen_server).
-compile([{parse_transform, lager_transform}]).

%% API
-export([
    csv_headers/0,
    lookup_ip/1,
    start_link/0,
    tags/0,
    to_csv/1
]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(QUERIES_PER_MINUTE, 150).
-define(ONE_MINUTE, 60000).
-define(NUM_ELEMENTS, 12).

-record(state, {
    enabled    = false     :: boolean(),
    lookups    = 0         :: integer(),
    start_time = undefined :: calendar:timestamp()
}).


%%%===================================================================
%%% API
%%%===================================================================

-spec tags() -> [atom()].
tags() -> [as, city, country, countryCode, isp, lat,
           lon, org, region, regionName, timezone, zip].

%%--------------------------------------------------------------------
%% @doc
%% Generate a CSV header, if IP-lookup is enabled.
%%
%% @end
%%--------------------------------------------------------------------
-spec csv_headers() -> string().
csv_headers() ->
    gen_server:call({global, ?SERVER}, csv_headers).

%%--------------------------------------------------------------------
%% @doc
%% Convert to a CSV record
%%
%% @end
%%--------------------------------------------------------------------
to_csv({ok, []}) ->
    "";
to_csv({ok, Result}) ->
    csv_row_if_valid(proplists:get_value(status, Result), Result);
to_csv(Err) ->
    lager:error("~p", [Err]),
    csv_row_if_valid([], []).

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
%% Look up an IP, if IP-lookup is enabled.
%%
%% @end
%%--------------------------------------------------------------------
-spec lookup_ip(IP :: string()) ->
    {ok, string()} | {error, {string(), integer(), list(), string()}} | term().
lookup_ip(IP) ->
    gen_server:call({global, ?SERVER}, {lookup, IP}, ?ONE_MINUTE + 1000).

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
    Enabled = application:get_env(deploy_track, geolocation, false),
    {ok, #state{enabled = Enabled}}.

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
handle_call(csv_headers, _From, State = #state{enabled = Enabled}) ->
    Reply = case Enabled of true ->
            string:join(lists:map(fun(X) ->
            Title = map_tag_to_title(X),
            deploy_track_util:flat_format("\"~s\"", [Title])
                              end,
            tags()), ",");
        _ ->
            ""
    end,
    {reply, Reply, State};
handle_call({lookup, IP}, _From, State = #state{enabled = Enabled}) ->
    {Result, NewState} =
        case Enabled of
             true ->
                {send_request(IP), delay_request(State)};
            _ ->
                {{ok, []}, State}
        end,
    {reply, Result, NewState};
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
-spec csv_row_if_valid(string(), deploy_track_util:proplist()) -> string().
csv_row_if_valid(<<"success">>, Result) ->
    Values = lists:map(fun(Key) ->
        Value = proplists:get_value(Key, Result),
        deploy_track_util:flat_format("\"~s\"",
            [deploy_track_util:stringify(Value)])
                       end, tags()),
    string:join(Values, ",");
csv_row_if_valid(_, _) ->
    blank_csv_row().

-spec blank_csv_row() -> string().
blank_csv_row() ->
    string:join(lists:duplicate(?NUM_ELEMENTS, "\"\""), ",").

-spec map_tag_to_title(atom()) -> string().
map_tag_to_title(as) -> "As";
map_tag_to_title(city) -> "City";
map_tag_to_title(country) -> "Country";
map_tag_to_title(countryCode) -> "Country Code";
map_tag_to_title(isp) -> "ISP";
map_tag_to_title(lat) -> "Latitude";
map_tag_to_title(lon) -> "Longitude";
map_tag_to_title(org) -> "Organization";
map_tag_to_title(region) -> "Region";
map_tag_to_title(regionName) -> "Region Name";
map_tag_to_title(timezone) -> "Time Zone";
map_tag_to_title(zip) -> "Postal Code".

%% @doc Send an HTTP request to the server
-spec send_request(IP :: string()) ->
    {ok, string()} | {error, {string(), integer(), list(), string()}} | term().
send_request(IP) ->
    Template = "/json/:ip",
    Params = [{ip, IP}],
    deploy_track_util:send_request("http://ip-api.com", get, Params,
        Template, []).

%% @doc Obey the rules of requests per minute
-spec delay_request(#state{}) -> #state{}.
delay_request(State = #state{lookups = ?QUERIES_PER_MINUTE,
                             start_time = StartTime}) ->
    Now = erlang:system_time(seconds),
    case (Now - StartTime) < 60 of
        %% If a minute has not elapsed, just sleep on it to be safe
        true ->
            lager:debug("Exceeded the request rate. Sleeping", []),
            timer:sleep(?ONE_MINUTE);
        _ -> ok
    end,
    State#state{lookups = 1,
                start_time = erlang:system_time(seconds)};
delay_request(State = #state{lookups = 0}) ->
    State#state{lookups = 1,
                start_time = erlang:system_time(seconds)};
delay_request(State = #state{lookups = Lookups}) ->
    State#state{lookups = Lookups + 1}.
