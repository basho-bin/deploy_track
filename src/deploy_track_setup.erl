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
%% Does a 1-time initialization of Mnesia during install.
%% Not an integral part of the Deployment Tracker
%% -------------------------------------------------------------------

-module(deploy_track_setup).
-author("hazen").

-include("deploy_track.hrl").
-define(RETRIES, 5).
-define(DELAY, 10000).
-define(TIMEOUT, 5000).
-define(IDLE_NODE, timer:seconds(90)).

%% API
-export([
    mnesia_init/5
]).

%% Called from the shell, so assume it's a single list of strings
-spec mnesia_init([node()], string(), string(), string(), string()) -> ok.
mnesia_init(Nodes, S3Key, S3Value, PkgCloudKey, PkgCloudValue) ->
    [Leader|_Rest] = Nodes,
    %% Let the first node do all of the driving
    case node() of
        Leader -> create_mnesia(Nodes, S3Key, S3Value, PkgCloudKey, PkgCloudValue);
        _ -> timer:sleep(?IDLE_NODE)
    end,
    ok.

%% Do all the steps necessary to create and populate the Mnesia DB
-spec create_mnesia([node()], string(), string(), string(), string()) -> ok.
create_mnesia(Nodes, S3Key, S3Value, PkgCloudKey, PkgCloudValue) ->
    ok = wait_until_nodes_are_up(Nodes),
    ok = wait_until_schema_created(Nodes),
    ok = wait_until_mnesia_running(Nodes),
    {atomic, ok} = mnesia:create_table(?CHECKPOINT_TABLE, [{attributes, record_info(fields, ?CHECKPOINT_TABLE)}, {disc_copies, Nodes}, {type, set}]),
    {atomic, ok} = mnesia_write(S3Key, S3Value),
    {atomic, ok} = mnesia_write(PkgCloudKey, PkgCloudValue),
    ok.

%% Write key/value pair to Mnesia
-spec mnesia_write(string(), string()) -> {'aborted', term()} | {'atomic', term()}.
mnesia_write(Key, Value) ->
    Tran = fun() ->
                mnesia:write(?CHECKPOINT_TABLE,
                             #checkpoint{key = Key, value = Value},
                             sticky_write)
           end,
    mnesia:transaction(Tran).

%% Verify all nodes are running
-spec wait_until_nodes_are_up([node()]) -> ok.
wait_until_nodes_are_up(Nodes) ->
    Fun = fun(Node) ->
        Result = rpc:call(Node, net_adm, ping, [Node], ?TIMEOUT),
        case Result of
            pong -> true;
            _ -> false
        end
    end,
    wait_until(Nodes, Fun).

%% Create Mnesia schema on all nodes
-spec wait_until_schema_created([node()]) -> ok.
wait_until_schema_created(Nodes) ->
    Fun = fun(Node) ->
        Result = rpc:call(Node, mnesia, create_schema, [Nodes], ?TIMEOUT),
        case Result of
            {badrpc, _} -> false;
            _ -> true
        end
    end,
    wait_until(Nodes, Fun).

%% Start Mnesia on all nodes
-spec wait_until_mnesia_running([node()]) -> ok.
wait_until_mnesia_running(Nodes) ->
    Fun = fun(Node) ->
        Result = rpc:call(Node, mnesia, start, [], ?TIMEOUT),
        case Result of
            {badrpc, _} -> false;
            _ -> true
        end
    end,
    wait_until(Nodes, Fun).

%% @doc Wrapper to verify `Fun' against multiple nodes. The function `Fun' is passed
%%      one of the `Nodes' as argument and must return a `boolean()' declaring
%%      whether the success condition has been met or not.
-spec wait_until([node()], fun((node()) -> boolean())) -> ok.
wait_until(Nodes, Fun) ->
    lists:map(
        fun(Node) ->
            ok = wait_until(Fun, Node, ?RETRIES, ?DELAY)
        end, Nodes),
    ok.

%% @doc Retry `Fun' until it returns `Retry' times, waiting `Delay'
%% milliseconds between retries. This is our eventual consistency bread
%% and butter
-spec wait_until(fun((node()) -> boolean()), node(), integer(), integer()) -> ok | {fail, _}.
wait_until(Fun, Node, Retry, Delay) when Retry > 0 ->
    Res = Fun(Node),
    case Res of
        true ->
            ok;
        _ when Retry == 1 ->
            {fail, Res};
        _ ->
            timer:sleep(Delay),
            wait_until(Fun, Node, Retry-1, Delay)
    end.
