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
-include("deploy_track.hrl").

%% Tests
-export([
    test_pkgcloud_filter_keys/1,
    test_pkgcloud_parse_details/1,
    test_s3_parse_record/1,
    test_s3_parse_put_record/1,
    test_url_substitution1/1,
    test_url_substitution2/1,
    test_url_substitution_fail/1
]).

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
    [{url_format_group,
      [],
      [test_url_substitution1,
       test_url_substitution2,
       test_url_substitution_fail]},
     {s3_format_group,
      [],
      [test_s3_parse_record,
       test_s3_parse_put_record]},
     {pkgcloud_group,
      [],
      [test_pkgcloud_parse_details,
       test_pkgcloud_filter_keys
      ]}].

all() -> [{group, url_format_group},
          {group, s3_format_group},
          {group, pkgcloud_group}].

test_url_substitution1(_Config) ->
    Param = [{alpha, "Uno"}, {beta, "Dos"}, {gamma, "Tres"}],
    Template = "foo/bar/:alpha/biz/:beta/bing",
    Result = deploy_track_util:format_url(Param, Template),
    Result == "foo/bar/Uno/biz/Dos/bing".

test_url_substitution2(_Config) ->
    Param = [{so, "Eins"}, {do, "Zwei"}, {fa, "Drei"}],
    Template = ":so/biz/:do/:fa/ling.suffix",
    Result = deploy_track_util:format_url(Param, Template),
    Result == "Eins/biz/Zwei/Drei/ling.suffix".

test_url_substitution_fail(_Config) ->
    Param = [{so, "Eins"}, {do, "Zwei"}, {fa, "Drei"}],
    Template = ":not_found/dolally",
    assert_fail(fun deploy_track_util:format_url/2, [Param, Template],
                throw, {missing_param, not_found}, "Expected substitution failure").

test_s3_parse_record(_Config) ->
    Record = [{etag,"\"048dc9fb0a19f985509d5cf190d861ee\""},
        {content_length,"407"},
        {content_type,"text/plain"},
        {content_encoding,undefined},
        {delete_marker,false},
        {version_id,"null"},
        {content,<<"1f724fbee7435abdbc971a75b7b107a67c6a42b1500bb3748e4032234edef530 downloads.basho.com [09/Mar/2016:23:50:15 +0000] 54.183.189.253 - E1FF7190217C96B3 REST.GET.OBJECT riak/2.0/2.0.5/rhel/6/riak-2.0.5-1.el6.x86_64.rpm \"GET /downloads.basho.com/riak/2.0/2.0.5/rhel/6/riak-2.0.5-1.el6.x86_64.rpm HTTP/1.0\" 200 - 59269904 59269904 2291 107 \"-\" \"Wget/1.12 (linux-gnu)\" -">>}],
    Result = deploy_track_s3:parse_log_records("Foo", Record, ["riak"]),
    Result = [#analytics{key = "Foo",
                         source = s3,
                         timestamp = {{2016,3,9},{23,50,15}},
                         ip = "54.183.189.253",
                         filename = "riak/2.0/2.0.5/rhel/6/riak-2.0.5-1.el6.x86_64.rpm",
                         product = "riak",
                         version = "2.0.5",
                         release = "-",
                         os = "rhel",
                         os_version = "6",
                         user_agent = "Wget/1.12 (linux-gnu)"}].

test_s3_parse_put_record(_Config) ->
    Record = [{etag,"\"048dc9fb0a19f985509d5cf190d861ee\""},
        {content_length,"407"},
        {content_type,"text/plain"},
        {content_encoding,undefined},
        {delete_marker,false},
        {version_id,"null"},
        {content,<<"1f724fbee7435abdbc971a75b7b107a67c6a42b1500bb3748e4032234edef530 downloads.basho.com [17/May/2016:03:38:33 +0000] 10.133.159.23 3272ee65a908a7677109fedda345db8d9554ba26398b2ca10581de88777e2b61 4BEF7CF4CF88E597 REST.PUT.OBJECT log/access_log-2016-05-17-03-38-33-6DD33BC5D84453D2 \"PUT /downloads.basho.com/log/access_log-2016-05-17-03-38-33-6DD33BC5D84453D2 HTTP/1.1\" 200 - - 406 43 10 \"-\" \"aws-internal/3\" -">>}],
    [skip] = deploy_track_s3:parse_log_records("Zing", Record, ["riak"]).

test_pkgcloud_parse_details(_Config) ->
    Details = [{downloaded_at,<<"2016-05-06T20:26:38.000Z">>},
        {ip_address,<<"67.197.247.52">>},
        {user_agent,<<"urlgrabber/3.10 yum/3.4.3">>},
        {source,<<"cli">>},
        {read_token,null}],
    Package = [{name,<<"riak-ts">>},
        {distro_version,<<"el/7">>},
        {created_at,<<"2016-05-05T00:52:17.000Z">>},
        {version,<<"1.3.0">>},
        {release,<<"1.el7.centos">>},
        {epoch,0},
        {private,false},
        {type,<<"rpm">>},
        {filename,<<"riak-ts-1.3.0-1.el7.centos.x86_64.rpm">>},
        {uploader_name,<<"basho">>},
        {indexed,true},
        {repository_html_url,<<"/basho/riak-ts">>},
        {package_url,<<"/api/v1/repos/basho/riak-ts/package/rpm/el/7/riak-ts/x86_64/1.3.0/1.el7.centos.json">>},
        {package_html_url,<<"/basho/riak-ts/packages/el/7/riak-ts-1.3.0-1.el7.centos.x86_64.rpm">>}],
    PackageDetails = lists:append(Package, Details),
    Result = deploy_track_pkgcloud:build_analytics(PackageDetails),
    Result = #analytics{key = "2016-05-06T20:26:38.000Z riak-ts 1.3.0 67.197.247.52",
            source = pkgcloud,
            timestamp = {{2016,5,6},{20,26,38}},
            ip = "67.197.247.52",
            filename = "riak-ts-1.3.0-1.el7.centos.x86_64.rpm",
            product = "riak-ts",
            version = "1.3.0",
            release = "1.el7.centos",
            os = "el",
            os_version = "7",
            user_agent = "urlgrabber/3.10 yum/3.4.3"}.

test_pkgcloud_filter_keys(_Config) ->
    Details = [[{name,<<"riak-ts">>},
                {distro_version,<<"ubuntu/trusty">>},
                {created_at,<<"2016-05-05T00:53:46.000Z">>},
                {version,<<"1.3.0">>},
                {release,<<"1">>},
                {epoch,0},
                {private,false},
                {type,<<"deb">>},
                {filename,<<"riak-ts_1.3.0-1_amd64.deb">>},
                {uploader_name,<<"basho">>},
                {indexed,true},
                {repository_html_url,<<"/basho/riak-ts">>},
                {package_url,<<"/api/v1/repos/basho/riak-ts/package/deb/ubuntu/trusty/ri">>},
                {package_html_url,<<"/basho/riak-ts/packages/ubuntu/trusty/riak-ts_1.3.0-">>},
                {downloaded_at,<<"2016-05-05T01:10:45.000Z">>},
                {ip_address,<<"73.4.30.67">>},
                {user_agent,<<"Debian APT-CURL/1.0 (1.0.1ubuntu2)">>},
                {source,<<"cli">>},
                {read_token,null}],
               [{name,<<"riak-ts">>},
                {distro_version,<<"ubuntu/trusty">>},
                {created_at,<<"2016-05-05T00:53:46.000Z">>},
                {version,<<"1.3.0">>},
                {release,<<"1">>},
                {epoch,0},
                {private,false},
                {type,<<"deb">>},
                {filename,<<"riak-ts_1.3.0-1_amd64.deb">>},
                {uploader_name,<<"basho">>},
                {indexed,true},
                {repository_html_url,<<"/basho/riak-ts">>},
                {package_url,<<"/api/v1/repos/basho/riak-ts/package/deb/ubuntu/trust">>},
                {package_html_url,<<"/basho/riak-ts/packages/ubuntu/trusty/riak-ts_1.">>},
                {downloaded_at,<<"2016-06-02T13:45:13.000Z">>},
                {ip_address,<<"158.85.240.149">>},
                {user_agent,<<"Debian APT-CURL/1.0 (1.0.1ubuntu2)">>},
                {source,<<"cli">>},
                {read_token,null}]],
        %% Should skip only the first item in the list
        Check1 = deploy_track_pkgcloud:build_key(hd(Details)),
        1 = length(deploy_track_pkgcloud:filter_out_logged_events(Details, Check1)),
        Check2 = deploy_track_pkgcloud:build_key(hd(tl(Details))),
        %% Since this is the last item, the list should be empty
        0 = length(deploy_track_pkgcloud:filter_out_logged_events(Details, Check2)).

assert_fail(Fun, Args, ExceptionType, ExceptionValue, Reason) ->
    try apply(Fun, Args) of
        _ -> ct:fail(Reason)
    catch
        ExceptionType:ExceptionValue -> ok
    end.
