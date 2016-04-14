-module('epkgcloud').

%% API exports
-export([new/1, repos/1, packages/1, packages/2,
	 downloads_count/1, downloads_count/2,
	 downloads_details/1, downloads_details/2,
	 repo_downloads_count/1, repo_downloads_count/2,
	 repo_downloads_details/1, repo_downloads_details/2,
	 installs_count/1, installs_count/2,
	 installs_details/1, installs_details/2,
	 show_package/1, show_package/2]).

-record(ctx, {base_url,
	      params = []}).


%%====================================================================
%% API functions
%%====================================================================

new(Params) ->
    ok = application:ensure_started(ibrowse),
    ApiKey = proplists:get_value(api_key, Params, <<>>),
    {ok, #ctx{base_url = base_url(ApiKey),
	      params = Params}}.


%% Return all of the repos for a user
repos(Ctx) ->
    req(Ctx, [], get, "/api/v1/repos").

packages(Ctx) ->
    packages(Ctx, []).

packages(Ctx, Params) ->
    req(Ctx, Params, get, "/api/v1/repos/:user/:repo/packages.json").

downloads_count(Ctx) ->
    downloads_count(Ctx, []).

downloads_count(Ctx, Params) ->
    case req(Ctx, Params, get,
	     "/api/v1/repos/:user/:repo/package/:type/:distro/"
	     ":version/:arch/:package/:package_version/:release/"
	     "stats/downloads/count.json") of
	{error, {_Url, "404", _RespHdrs, _RespBody}} ->
	    {ok, []};
	Resp ->
	    Resp
    end.

downloads_details(Ctx) ->
    downloads_details(Ctx, []).

downloads_details(Ctx, Params) ->
    case req(Ctx, Params, get,
	     "/api/v1/repos/:user/:repo/package/:type/:distro/"
	     ":version/:package/:arch/:package_version/:release/"
	     "stats/downloads/detail.json") of
	{error, {_Url, "404", _RespHdrs, _RespBody}} ->
	    {ok, []};
	Resp ->
	    Resp
    end.


repo_downloads_count(Ctx) ->
    repo_downloads_details(Ctx, []).
   
repo_downloads_count(Ctx, Params) ->
    {ok, Packages} = packages(Ctx, Params),
    %% Packages = lists:sublist(Packages0, 3),
    [element(2, downloads_count(Ctx, package_to_download_details_params(P) ++ Params)) || P <- Packages].


repo_downloads_details(Ctx) ->
    repo_downloads_details(Ctx, []).
   
repo_downloads_details(Ctx, Params) ->
    {ok, Packages} = packages(Ctx, Params),
    %% Packages = lists:sublist(Packages0, 3),
    [element(2, downloads_details(Ctx, package_to_download_details_params(P) ++ Params)) || P <- Packages].




installs_count(Ctx) ->
    installs_count(Ctx, []).

installs_count(Ctx, Params) ->
    URL = case param(distro, Ctx, Params) of
	      {ok, _} ->
		  case param(version, Ctx, Params) of
		      {ok, _} ->
			  "/api/v1/repos/:user/:repo/stats/installs/:distro/:version/count.json";
		      _ ->
			  "/api/v1/repos/:user/:repo/stats/installs/:distro/count.json"
		  end;
	      _ ->
		   "/api/v1/repos/:user/:repo/stats/installs/count.json"
	  end,
	
    case req(Ctx, Params, get, URL) of
	{error, {_Url, "404", _RespHdrs, _RespBody}} ->
	    {ok, []};
	Resp ->
	    Resp
    end.


installs_details(Ctx) ->
    installs_details(Ctx, []).

installs_details(Ctx, Params) ->
    URL = case param(distro, Ctx, Params) of
	      {ok, _} ->
		  case param(version, Ctx, Params) of
		      {ok, _} ->
			  "/api/v1/repos/:user/:repo/stats/installs/:distro/:version/detail.json";
		      _ ->
			  "/api/v1/repos/:user/:repo/stats/installs/:distro/detail.json"
		  end;
	      _ ->
		   "/api/v1/repos/:user/:repo/stats/installs/detail.json"
	  end,
	
    case req(Ctx, Params, get, URL) of
	{error, {_Url, "404", _RespHdrs, _RespBody}} ->
	    {ok, []};
	Resp ->
	    Resp
    end.


show_package(Ctx) ->
    show_package(Ctx, []).

show_package(Ctx, Params) ->
    URL = "/api/v1/repos/:user/:repo/package/:type/:distro/:version/:arch/:package/:package_version/:release.json",
    req(Ctx, Params, get, URL).


%%====================================================================
%% Internal functions
%%====================================================================

base_url(ApiKey) ->
    lists:flatten(io_lib:format("https://~s:@packagecloud.io", [ApiKey])).    

req(#ctx{base_url = BaseUrl, params = CtxParams}, ReqParams, Method, PathTemplate) ->
    %% substitute uses proplists and will take first instance of a parameter,
    %% so request parmeters override
    Path = substitute(ReqParams ++ CtxParams, PathTemplate),
    Url = BaseUrl ++ Path,
    case ibrowse:send_req(Url, [], Method,  [], [{response_format, binary}]) of
	{ok, "200", _RespHdrs, RespBody} ->
	    {ok, jsx:decode(RespBody, [{labels, atom}])};
	{ok, Code, RespHdrs, RespBody} ->
	    {error, {Url, Code, RespHdrs, RespBody}};
	ER ->
	    ER
    end.

param(Name, #ctx{params = CtxParams}, ReqParams) ->
    case proplists:lookup(Name, ReqParams ++ CtxParams) of % first entry takes precedence
	{Name, Value} ->
	    {ok, Value};
	_ ->
	    undefined
    end.



substitute(Params, Template) ->
    {Template2, Suffix} =
	case filename:extension(Template) of
	    [] ->
		{Template, ""};
	    Ext ->
		{filename:rootname(Template, Ext), Ext}
	end,
    [$/ | string:join([sub(Params, C) || C <- string:tokens(Template2, "/")], "/")] ++ Suffix.

sub(Params, [$: | NameStr]) ->
    Name = list_to_existing_atom(NameStr),
    case proplists:is_defined(Name, Params) of
	true ->
	    stringify(proplists:get_value(Name, Params));
	false ->
	    throw({missing_param, Name})
    end;
sub(_Params, Component) ->
    Component.

    
stringify(Term) when is_integer(Term) ->
    lists:flatten(io_lib:format("~b", [Term]));
stringify(Term) when is_float(Term) ->
    lists:flatten(io_lib:format("~f", [Term]));
stringify(Term) ->
    lists:flatten(io_lib:format("~s", [Term])).


%% Parse extra information out of the package
%%  package_url seems to have it all  "/api/v1/repos/basho/riak/package/deb/debian/squeeze/riak/amd64/2.0.5-1.json",
%% type
%% distro
%% version
%% arch
%% package
%% package_version
%% release
package_to_download_details_params(PackageObj) ->
    PackageUrl = proplists:get_value(package_url, PackageObj),
    [_Api, <<"v1">>, <<"repos">>, User, Repo, <<"package">>,
     Type, Distro, Version, Package, Arch | _Rest ] =
	binary:split(PackageUrl, <<"/">>, [global, trim_all]),
    Package = proplists:get_value(name, PackageObj), % Paranoia for decode - validation
    PackageVersion = proplists:get_value(version, PackageObj),
    [Release | _] = binary:split(proplists:get_value(release, PackageObj), <<".">>),
    [{user, User}, {repo, Repo}, {type, Type}, {distro, Distro}, {version, Version},
     {arch, Arch}, {package, Package}, {package_version, PackageVersion}, {release, Release}].
