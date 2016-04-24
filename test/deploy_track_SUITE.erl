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
%% This module is responsible for testing various aspects of
%% the Deployment Tracker
%%
%% -------------------------------------------------------------------

-module(deploy_track_SUITE).
-author("hazen").

-include_lib("common_test/include/ct.hrl").

%% Tests
-export([test1/1, test2/1]).

%% Common Test API
-export([
    all/0,
    end_per_group/2,
    end_per_suite/1,
    end_per_testcase/2,
    groups/0,
    init_per_group/2,
    init_per_suite/1,
    init_per_testcase/2,
    suite/0
]).

suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [{simple_group,
      [],
      [test1, test2]}].

all() -> [{group, simple_group}].

test1(_Config) ->
    1 = 1.

test2(_Config) ->
    A = 0,
    1/A.
