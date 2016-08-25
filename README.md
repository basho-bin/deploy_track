# deploy_track
Package download analytics

# Docs

See https://docs.google.com/document/d/1xGtmbk5jwhS4u1tH-iPUObL7o-OgRvhTbv-MpUDhJEo/edit?userstoinvite=phagan@basho.com&ts=5710147d&actionButton=1#heading=h.1n5oaoo3fkvo

# 1-Time Setup Mnesia Database
This process will be required after each deployment
```bash
#Bring up both nodes as user "ubuntu"
erl -setcookie deploy_track -name deploy_track
```
```erlang
%% Run on each node
rr("/opt/deploy_track/include/deploy_track.hrl").
application:set_env(mnesia, dir, "/opt/deploy_track/mnesia").
NodeList = ['deploy_track@tools.az1.cloud1','deploy_track@tools.az2.cloud1'].
mnesia:create_schema(NodeList).
%% Expected: {error,{'deploy_track@tools.az1.cloud1',{already_exists,'deploy_track@tools.az1.cloud1'}}}
%% Verify both directories have been created, then start
mnesia:start().
%% Once running on both nodes, get info
mnesia:info().
%% On one machine:
mnesia:create_table(checkpoint,[{attributes, record_info(fields, checkpoint)}, {disc_copies, NodeList},{type, set}]).
```
Add initial values to Mnesia
```erlang
    Tran = fun() ->
        ok = mnesia:write(checkpoint,
            #checkpoint{key = "s3-debug", value = "log/access_log-2016-04-27-00-20-27-A880247B87E5017E"},
            sticky_write)
           end,
    mnesia:transaction(Tran).
    Tran1 = fun() ->
        ok = mnesia:write(checkpoint,
            #checkpoint{key = "pkgcloud-debug", value = "2016-05-05T01:03:34.000Z riak-ts 1.3.0 54.166.165.133"},
            sticky_write)
           end,
    mnesia:transaction(Tran1).
```
