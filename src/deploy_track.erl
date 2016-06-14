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

-module(deploy_track).
-author("hazen").

-behaviour(application).

%% Application callbacks
-export([
    resolve_deps/1,
    start/0, start/2,
    stop/1
]).

%% Stolen from https://github.com/basho/giddyup
-spec start() -> ok.
start() ->
    lists:foreach(
        fun(Dep) ->
            case is_otp_base_app(Dep) of
                true -> ok;
                _ -> application:start(Dep)
            end
       end, resolve_deps(deploy_track)),
    ok.

-spec dep_apps(App :: atom()) -> [atom()].
dep_apps(App) ->
    application:load(App),
    {ok, Apps} = application:get_key(App, applications),
    Apps.

-spec all_deps(App :: atom(), Deps :: [atom()]) -> [[atom()]].
all_deps(App, Deps) ->
    [[ all_deps(Dep, [App|Deps]) || Dep <- dep_apps(App),
        not lists:member(Dep, Deps)], App].

-spec resolve_deps(App :: atom()) -> [atom()].
resolve_deps(App) ->
    DepList = all_deps(App, []),
    {AppOrder, _} = lists:foldl(fun(A,{List,Set}) ->
        case sets:is_element(A, Set) of
            true ->
                {List, Set};
            false ->
                {List ++ [A], sets:add_element(A, Set)}
        end
                                end,
        {[], sets:new()},
        lists:flatten(DepList)),
    AppOrder.

-spec is_otp_base_app(App :: atom()) -> boolean().
is_otp_base_app(kernel) -> true;
is_otp_base_app(stdlib) -> true;
is_otp_base_app(_) -> false.

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
start(normal, _StartArgs) ->
    %% TODO: Ensure services are available here
    deploy_track_config:extract_env(),
    deploy_track_sup:start_link();
start({takeover, _OtherNode}, _StartArgs) ->
    deploy_track_sup:start_link().

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
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
