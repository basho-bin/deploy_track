%%
%% Send data to google analytics
%%

%% {ok,Pid}=ganalytics:start_link([{tid,"UA-XXXXXXXX-XX"},{verbose,true}]).
%% {ok,Pid}=ganalytics:start_link([{tid,"UA-XXXXXXXX-XX"},{verbose,true}, {url,"https://www.google-analytics.com/debug/collect"}]).
%% unlink(Pid),
%% ganalytics:send([{cid,1234},{t,event},{ec,deply},{ea,install}]).



-module(ganalytics).
-export([start_link/1, send/1, flush/0]).
-export([uuid_string/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-record(state, {verbose,         %% true or false - log interactions
		url,             %% URL to hit (can override for 
		%% https://www.google-analytics.com/debug/collect)
		tid :: binary(), %% tracking id
		acc = "",        %% accumulated list to send
		hits = 0,        %% Accumulated hits
		batch_bytes = 0, %% Accumulated bytes
	        max_hits = 20,   %% Maximum hits - set to zero to send on each send/1 call.
	        max_batch_bytes = 16384,
                max_payload_bytes = 8192}).

start_link(Params) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Params, []).

%% Send a proplist of analytics information.
%% API version and tid will automatically be added
send([]) ->
    ok;
send([H|_T] = Batch) when is_list(H) ->
    gen_server:call(?MODULE, {send, Batch}, 60000);
send(Payload) ->
    send([Payload]). % Convert to a batch

%% Make sure all pending batches have been transmitted.
flush() ->
    gen_server:call(?MODULE, flush, 60000).

%%
%% Initialize Google Analytics
%% Params
%%   tid - tracking id, provided by Google
%%
init(Params) ->
    ok = application:ensure_started(ibrowse),
    {ok, #state{verbose = get_default_param(verbose, Params, false),
		url = get_default_param(url, Params, "http://www.google-analytics.com/batch"),
		tid = get_param(tid, Params),
		max_hits = get_default_param(max_hits, Params, 20)}}.


handle_call({send, Batch}, _From, State) ->
    {Reply, State2} = encode_and_maybe_send(Batch, State),
    {reply, Reply, State2};
handle_call(flush, _From, State) ->
    {reply, do_send(State), reset(State)};
handle_call(_Msg, _From, State) ->
    error_logger:info_msg("Unhandled msg - ~p", [_Msg]),
    {noreply, State}.

handle_cast(_Msg, State) ->
    error_logger:info_msg("Unhandled cast msg - ~p", [_Msg]),
    {noreply, State}.

handle_info(_Msg, State) ->
    error_logger:info_msg("Unhandled info msg - ~p", [_Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Get parameter value, first look in Params proplist
%% then in the application environment.  If not present
%% crash.
get_param(Name, Params) ->
    case proplists:is_defined(Name, Params) of
	true ->
	    proplists:get_value(Name, Params);
	false ->
	    case application:get_env(Name) of
		undefined ->
		    error({param_missing, Name});
		{ok, Value} ->
		    Value
	    end
    end.

%% Get parameter value, first look in Params proplist
%% then in the application environment.  If not present
%% crash.
get_default_param(Name, Params, Default) ->
    case proplists:is_defined(Name, Params) of
	true ->
	    proplists:get_value(Name, Params);
	false ->
	    case application:get_env(Name) of
		undefined ->
		    Default;
		{ok, Value} ->
		    Value
	    end
    end.
	    
%%
%% Encode the payload and if it puts over the hits or batch size
%% limits, send the existing data and accumulat the new entry.
%%
encode_and_maybe_send([], State) ->
    {ok, State};
encode_and_maybe_send([Payload | Rest], #state{tid = Tid} = State) ->
    Encoded = encode_payload([{v, 1}, {tid, Tid} | Payload]),
    case maybe_send(Encoded, length(Encoded), State) of
	{ok, State2} ->
	    encode_and_maybe_send(Rest, State2);
	ER ->
	    ER
    end.

%%
%% Check if we have hit batch limits, if so, send, otherwise just add
%%
maybe_send(Encoded, Length, #state{max_hits = 0} = State) -> % Batching disabled, just send
    case do_send(add(Encoded, Length, State)) of
	ok ->
	    {ok, reset(State)};
	ER ->
	    ER
    end;
maybe_send(Encoded, Length, #state{hits = Hits,  % if one more pushes over the max hits
				   max_hits = MaxHits} = State) when Hits + 1 >= MaxHits ->
    send_and_add(Encoded, Length, State);
maybe_send(Encoded, Length, #state{batch_bytes = BatchBytes, % if too large when included
				   max_batch_bytes = MaxBatchBytes} = State)
  when Length + BatchBytes >= MaxBatchBytes ->
    send_and_add(Encoded, Length, State);
maybe_send(_Encoded, Length, #state{max_payload_bytes = MaxPayloadBytes} = State)
  when Length > MaxPayloadBytes->
    {{error, {payload_too_large, Length}}, State};
maybe_send(Encoded, Length, State) ->
    {ok, add(Encoded, Length, State)}.

%%
%% Add the new entry to the accumulator and update counts/fields
%%
add(Encoded, Length, #state{acc = Acc, hits = Hits, 
			    batch_bytes = Bytes} = State) ->
    State#state{acc = Acc ++ Encoded, hits = Hits + 1,
		batch_bytes = Bytes + Length}.

%% 
%% Send to Google analytics, reset the state and add the new entry
%%
send_and_add(Encoded, Length, State) ->
    case do_send(State) of
	ok ->
	    {ok, add(Encoded, Length, reset(State))};
	ER ->
	    {ER, State}
    end.

%%
%% Reset all non-parameters in the state
%%
reset(State) ->
    State#state{acc = "", hits = 0, batch_bytes = 0}.

%%
%% Post to google analytics
%% 
do_send(#state{url = URL, acc = Acc} = State) ->
    verbose(State, "Posting to ~p\n~p\n", [URL, Acc]),
    Response = ibrowse:send_req(URL, [], post, Acc),
    verbose(State, "Response ~p\n", [Response]),
    case Response of
	{ok, [$2 | _], _RespHdr, _RespBody} -> %% Check a 2xx
	    ok;
	{ok, Status, _RespHdr, _RespBody} ->
	    {error, {non_2xx, Status}};
	ER ->
	    ER
    end.
    


%%
%% Encode a payload.  Required values, not enforced
%%  v // version
%%  tid // Tracking ID / Property Id
%%  cid // Anonymous client ID
%%  t // Hit Type
%%  uip // User-supplied IP
%%
encode_payload(Payload) ->
    string:join([string:join([stringify(P), stringify(V)], "=") || {P, V} <- Payload], "&")
	++ "\r\n".


stringify(Term) when is_integer(Term) ->
    http_uri:encode(lists:flatten(io_lib:format("~b", [Term])));
stringify(Term) when is_float(Term) ->
    http_uri:encode(lists:flatten(io_lib:format("~f", [Term])));
stringify(Term) ->
    http_uri:encode(lists:flatten(io_lib:format("~s", [Term]))).


verbose(#state{verbose = false}, _Fmt, _Args) ->
    ok;
verbose(#state{verbose = true}, Fmt, Args) ->
    error_logger:info_msg(Fmt, Args).


uuid_string(U) ->
    lists:flatten(io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~2.16.0b~2.16.0b-~12.16.0b", get_parts(U))).

get_parts(<<TL:32, TM:16, THV:16, CSR:8, CSL:8, N:48, _/binary>>) ->
    [TL, TM, THV, CSR, CSL, N].
