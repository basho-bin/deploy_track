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
%% This module deals with setting up the environmental
%% configuration.
%%
%% -------------------------------------------------------------------

-module(deploy_track_config).
-author("hazen").

-compile([{parse_transform, lager_transform}]).

%% API
-export([
    env_or_default/2,
    extract_env/0,
    hostname/0]).

-spec extract_env() -> ok.
extract_env() ->
    CfgEnv = extract_from_cfg_file(config_file(), deploy_track),
    OSEnv = extract_os_env(),

    %% The OS environment supersedes the config file
    Environment = OSEnv ++ CfgEnv,

    lists:foreach(
        fun({Key,Value}) ->
            application:set_env(deploy_track, Key, Value)
        end, Environment),
    ok.

%% Shamelessly stolen from https://github.com/basho/giddyup
-spec env_or_default(Key :: string(),
                     Default :: term()) -> term().
env_or_default(Key, Default) ->
    case os:getenv(Key) of
        false -> Default;
        Val -> Val
    end.

-spec hostname() -> string().
hostname() ->
    os:getenv("HOSTNAME").

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec extract_from_cfg_file(Filename :: string(),
                            App :: atom()) ->
                            deploy_track_util:proplist().
extract_from_cfg_file(Filename, App) ->
    io:format(user, "Extracting config from file ~p~n", [Filename]),
    {ok,[Cfg]} = file:consult(Filename),
    proplists:get_value(App, Cfg).

-spec extract_os_env() -> deploy_track_util:proplist().
extract_os_env() ->
    DownloadAccessKey = os:getenv("S3_DL_ACCESS_KEY"),
    DownloadSecretKey = os:getenv("S3_DL_SECRET_KEY"),
    UploadAccessKey = os:getenv("S3_UP_ACCESS_KEY"),
    UploadSecretKey = os:getenv("S3_UP_SECRET_KEY"),
    PkgCloudKey = os:getenv("PKGCLOUD_KEY"),
    AnalyticsTid = os:getenv("GA_TID"),

    [{s3_dl_access_key, DownloadAccessKey},
     {s3_dl_secret_key, DownloadSecretKey},
     {s3_up_access_key, UploadAccessKey},
     {s3_up_secret_key, UploadSecretKey},
     {pkgcloud_key, PkgCloudKey},
     {analytics_tid, AnalyticsTid}].

-spec config_file() -> string() | {error, term()}.
config_file() ->
    File = os:getenv("DEPLOY_TRACK_CONFIG"),
    case File of
        false ->
            io:format(user, "ERROR: DEPLOY_TRACK_CONFIG is not set~n", []),
            {error, config_env_not_set};
        _ ->
            File
    end.
