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
%% This module contains common utility functions.
%%
%% -------------------------------------------------------------------

-module(deploy_track_util).
-author("hazen").

-compile([{parse_transform, lager_transform}]).

%% API
-export([
    encode_query_string/1,
    ensure_services_are_up/1,
    ensure_lager_is_up/0,
    ensure_ssl_is_up/0,
    flat_format/2,
    format_url/2,
    ignore_not_found/1,
    iso_8601_date_format/1,
    mon_to_num/1,
    split_with_quotes/1,
    send_request/5, send_request/6,
    stringify/1,
    verify_checkpoint/1, verify_checkpoint/2
]).

-type property()     :: atom() | tuple().
-type proplist()     :: [property()].

-export_type([property/0, proplist/0]).

%% @doc Flatten a formatted list
-spec flat_format(Format :: string(), Args :: list()) -> string().
flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

%% @doc Convert any term into a string
-spec stringify(Term :: term()) -> string().
stringify(Term) when is_integer(Term) ->
    http_uri:encode(flat_format("~b", [Term]));
stringify(Term) when is_float(Term) ->
    http_uri:encode(flat_format("~f", [Term]));
stringify(Term) ->
    http_uri:encode(flat_format("~s", [Term])).


%% @doc Send an HTTP request to the server
-spec send_request(BaseUrl :: string(),
    Method :: atom(),
    Params :: deploy_track_util:proplist(), PathTemplate :: string(),
    QueryString :: deploy_track_util:proplist(),
    Payload :: string()) ->
    {ok, string()} | {error, {string(), string(), list(), string()}} | term().
send_request(BaseUrl, Method, Params, PathTemplate, QueryString) ->
    send_request(BaseUrl, Method, Params, PathTemplate, QueryString, []).

-spec send_request(BaseUrl :: string(),
    Method :: atom(),
    Params :: deploy_track_util:proplist(), PathTemplate :: string(),
    QueryString :: deploy_track_util:proplist()) ->
    {ok, string()} | {error, {string(), string(), list(), string()}} | term().
send_request(BaseUrl, Method, Params, PathTemplate, QueryString, Payload) ->
    Path = format_url(Params, PathTemplate),
    Url = BaseUrl ++ Path ++ maybe_add_query_string(QueryString),
    case ibrowse:send_req(Url, [], Method,  Payload, [{response_format, binary}]) of
        {ok, "200", _RespHdrs, RespBody} ->
            GIF89a = <<71,73,70,56,57,97>>,
            case RespBody of
                <<GIF89a:6/binary, _Rest/binary>> -> {ok, gif};
                _ -> {ok, jsx:decode(RespBody, [{labels, atom}])}
            end;
        {ok, Code, RespHdrs, RespBody} ->
            {error, {Url, Code, RespHdrs, RespBody}};
        ER ->
            ER
    end.

%% @doc Replace all instances of colon-prefixed values in the Template
%%      with values in the Params proplist
-spec format_url(Params :: deploy_track_util:proplist(),
    Template :: string()) -> string().
format_url(Params, Template) ->
    %% Strip off the suffix, if it is present
    {Template2, Suffix} =
        case filename:extension(Template) of
            [] ->
                {Template, ""};
            Ext ->
                {filename:rootname(Template, Ext), Ext}
        end,
    Replaced = [replace_named_element(Params, Elem) || Elem <- string:tokens(Template2, "/")],
    clean_up_url([$/ | string:join(Replaced, "/")] ++ Suffix).

clean_up_url("/") ->
    "";
clean_up_url(Url) ->
    Url.

-spec ignore_not_found(Result :: {error, {string(), string(), list(), string()}}) ->
    {ok, []} | {ok, term()} | term().
ignore_not_found(Result) ->
    case Result of
        {error, {_Url, "404", _RespHdrs, _RespBody}} ->
            {ok, []};
        Resp ->
            Resp
    end.

-spec maybe_add_query_string(deploy_track_util:proplist()) -> string().
maybe_add_query_string([]) ->
    [];
maybe_add_query_string(Payload) ->
    "?" ++ encode_query_string(Payload).

%% @doc Ensure the SSL service is running
-spec ensure_ssl_is_up() -> ok.
ensure_ssl_is_up() ->
    ensure_services_are_up([crypto, asn1, public_key, ssl]).

%% @doc Ensure the lager service is running
-spec ensure_lager_is_up() -> ok.
ensure_lager_is_up() ->
    ensure_services_are_up([compiler, syntax_tools, goldrush, lager]).

%% @doc Ensure services are running
-spec ensure_services_are_up([atom()]) -> ok.
ensure_services_are_up(Services) ->
    lists:foreach(fun(Mod) ->
        ok = application:ensure_started(Mod)
                  end, Services).

%% @doc Take a string and split into elements by space, preserving quoted spaces
-spec split_with_quotes(list(string())) -> list(string()).
split_with_quotes(List) ->
    split_with_quotes(string:tokens(List, " "), [], [], false).

-spec split_with_quotes(list(), Acc :: list(), Quoted :: list(),
                        InQuote :: boolean()) -> list().
split_with_quotes([], Acc, _Quoted, _InQuote) ->
    Acc;
split_with_quotes([H | T], Acc, Quoted, true) ->
    case lists:member($", H) of
        true ->
            %% We are closing a quote so add the quoted accumulator to the main one
            split_with_quotes(T, Acc ++ [remove_quotes(Quoted ++ " " ++ H)], [], false);
        _ ->
            %% We are still in a quoted string, so extend Quoted
            split_with_quotes(T, Acc, Quoted ++ " " ++ H, true)
    end;
%% Standard case not in a quote, yet...
split_with_quotes([H | T], Acc, [], false) ->
    case count_chr(H, $") of
        0 ->
            %% Just a normal string, no quotes
            split_with_quotes(T, Acc ++ [H], [], false);
        1 ->
            %% We are opening a quote so cache a new Quoted
            split_with_quotes(T, Acc, H, true);
        _ ->
            %% Hopefully this is a single, quoted word
            split_with_quotes(T, Acc ++ [remove_quotes(H)], [], false)
    end.

%% @doc Convert a 3-letter month to an integer
-spec mon_to_num(string()) -> integer().
mon_to_num("Jan") -> 1;
mon_to_num("Feb") -> 2;
mon_to_num("Mar") -> 3;
mon_to_num("Apr") -> 4;
mon_to_num("May") -> 5;
mon_to_num("Jun") -> 6;
mon_to_num("Jul") -> 7;
mon_to_num("Aug") -> 8;
mon_to_num("Sep") -> 9;
mon_to_num("Oct") -> 10;
mon_to_num("Nov") -> 11;
mon_to_num("Dec") -> 12.

%% @doc Create a human-readable standard date/time string
-spec iso_8601_date_format(calendar:datetime()) -> string().
iso_8601_date_format({{Y, M, D}, {H, Mi, S}}) ->
    flat_format("~b-~2..0b-~2..0bT~2..0b:~2..0b:~2..0bZ", [Y, M, D, H, Mi, S]).

%% @doc Add on a URL Query String
-spec encode_query_string(proplist()) -> string().
encode_query_string(Argument) ->
    string:join([string:join([stringify(P), stringify(V)], "=") || {P, V} <- Argument], "&").


%% Make sure that the checkpoint has not changed by another process
-spec verify_checkpoint(Marker :: string()) -> boolean().
verify_checkpoint(Marker) ->
    Checkpoint = deploy_track_s3:fetch_checkpoint(),
    Checkpoint == Marker.

%% Has someone moved my cheese? Did another process checkpoint?
%% It's more of a sanity check than a solid guarantee
-spec verify_checkpoint(Key :: string(),
    Marker :: string()) -> boolean().
verify_checkpoint(Key, Marker) ->
    Checkpoint = deploy_track_s3:fetch_checkpoint(Key),
    binary_to_list(Checkpoint) == Marker.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc If the Element string begins with a colon, look up its value in the the
%%      Params proplist.  Otherwise just return the string back.
-spec replace_named_element(Params :: deploy_track_util:proplist(),
    Element :: string()) -> string().
replace_named_element(Params, [$: | Element]) ->
    Name = list_to_existing_atom(Element),
    case proplists:is_defined(Name, Params) of
        true ->
            deploy_track_util:stringify(proplists:get_value(Name, Params));
        false ->
            throw({missing_param, Name})
    end;
replace_named_element(_Params, Element) ->
    Element.

-spec count_chr(String :: string(), Chr :: integer()) -> integer().
count_chr(String, Chr) ->
    F = fun(X, N) when X =:= Chr -> N + 1;
        (_, N)                -> N
        end,
    lists:foldl(F, 0, String).

-spec remove_quotes(S :: string()) -> string().
remove_quotes(S) ->
    lists:filtermap(fun(X) -> case X of $" -> false; _ -> {true, X} end end, S).
