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
%% -------------------------------------------------------------------

-module(deploy_track_app).
-author("hazen").

-behaviour(application).

%% Application callbacks
-export([
    start/0, start/2,
    stop/1
]).

-spec start() ->
    {ok, pid()} |
    {ok, pid(), State :: term()} |
    {error, Reason :: term()}.
start() ->
   start(normal, []).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Start the top-level supervisor for deploy_track.
%%
%% @end
%%--------------------------------------------------------------------
-spec(start(StartType :: normal | {takeover, node()} | {failover, node()},
    StartArgs :: term()) ->
    {ok, pid()} |
    {ok, pid(), State :: term()} |
    {error, Reason :: term()}).
start(_Type, _StartArgs) ->
    ok.
    %%deploy_track_sup:start_link().

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called whenever deploy_track has stopped. It
%% is intended to be the opposite of Module:start/2 and should do
%% any necessary cleaning up. The return value is ignored.
%%
%% @end
%%--------------------------------------------------------------------
-spec(stop(State :: term()) -> term()).
stop(_State) ->
    lager:info("Stopped application ~p", [?MODULE]),
    io:format("Stopped application ~p~n", [?MODULE]),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
