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

%% API
-export([
    env_or_default/2,
    extract_env/0]).

-spec extract_env() -> ok.
extract_env() ->
    DownloadAccessKey = os:getenv("S3_DL_ACCESS_KEY"),
    DownloadSecretKey = os:getenv("S3_DL_SECRET_KEY"),
    UploadAccessKey = os:getenv("S3_UP_ACCESS_KEY"),
    UploadSecretKey = os:getenv("S3_UP_SECRET_KEY"),
    PkgCloudKey = os:getenv("PKGCLOUD_KEY"),

    Environment = [
        {s3_dl_access_key, DownloadAccessKey},
        {s3_dl_secret_key, DownloadSecretKey},
        {s3_up_access_key, UploadAccessKey},
        {s3_up_secret_key, UploadSecretKey},
        {pkgcloud_key, PkgCloudKey}],

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
