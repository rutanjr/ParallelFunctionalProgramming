-module(map_reduce_dist).
-compile(export_all).

map_reduce_dist(Map,M,Reduce,R,Input) ->
    Parent = self(),
    Splits = split_into(M,Input),
    Nodes = nodes(),
    N = length(Nodes),
    SplitsZ = lists:zip(Splits,[lists:nth((I rem N) + 1,Nodes) 
				 || I <- lists:seq(0,M-1)]),    
    Mappers = 
	[spawn_mapper(Parent,Map,R,Split,Node)
	 || {Split,Node} <- SplitsZ],
    Mappeds = 
	[receive {Pid,L} -> L end || Pid <- Mappers],
    Reducers = 
	[spawn_reducer(Parent,Reduce,I,Mappeds,Nodes) 
	 || I <- lists:seq(0,R-1)],
    Reduceds = 
	[receive {Pid,L} -> L end || Pid <- Reducers],
    lists:sort(lists:flatten(Reduceds)).

spawn_mapper(Parent,Map,R,Split,Node) ->
    spawn_link(Node,fun() ->
			Mapped = [{erlang:phash2(K2,R),{K2,V2}}
				  || {K,V} <- Split,
				     {K2,V2} <- Map(K,V)],
			Parent ! {self(),map_reduce:group(lists:sort(Mapped))}
		end).

split_into(N,L) ->
    split_into(N,L,length(L)).

split_into(1,L,_) ->
    [L];
split_into(N,L,Len) ->
    {Pre,Suf} = lists:split(Len div N,L),
    [Pre|split_into(N-1,Suf,Len-(Len div N))].

spawn_reducer(Parent,Reduce,I,Mappeds,Nodes) ->   
    N = length(Nodes),
    Inputs = [KV
	      || Mapped <- Mappeds,
		 {J,KVs} <- Mapped,
		 I==J,
		 KV <- KVs],
    spawn_link(lists:nth(I rem N + 1,Nodes),
	       fun() -> Parent ! {self(),map_reduce:reduce_seq(Reduce,Inputs)} end).

%% map_reduce_dist(Map,M,Reduce,R,Input) ->
%%     Parent = self(),
%%     Ref = make_ref(),
%%     Splits = map_reduce:split_into(M,Input),
%%     Nodes = nodes(),
%%     N = length(Nodes),
%%     SplitsZ = lists:zip(Splits,[lists:nth((I rem N) + 1,Nodes) 
%% 				|| I <- lists:seq(0,M-1)]),    
%%     [rpc:call(Node,map_reduce_dist,spawn_mapper,[Parent,Map,R,Split,Ref])
%% 	 || {Split,Node} <- SplitsZ],
%%     Mappeds = 
%% 	[receive {Ref,L} -> L end || _ <- lists:seq(1,M)],
%%     ChunksZ = lists:zip(split_chunks(Mappeds,R),[lists:nth((I rem N) + 1,Nodes) 
%% 				|| I <- lists:seq(0,R-1)]),    
%%     [rpc:call(Node,map_reduce_dist,spawn_reducer,[Parent,Reduce,Chunk,Ref])
%%          || {Chunk,Node} <- ChunksZ],
%%     Reduceds = 
%% 	[receive {Ref,L} -> L end || _ <- lists:seq(1,R)],
%%     lists:sort(lists:flatten(Reduceds)).

%% split_chunks(Mappeds,R) ->
%%     [[KV
%%      || Mapped <- Mappeds,
%% 	{J,KVs} <- Mapped,
%% 	I==J,
%% 	KV <- KVs] || I <- lists:seq(0,R-1)].

%% spawn_mapper(Parent,Map,R,Split,Ref) ->
%%     spawn_link(fun() ->
%% 			Mapped = [{erlang:phash2(K2,R),{K2,V2}}
%% 				  || {K,V} <- Split,
%% 				     {K2,V2} <- Map(K,V)],
%% 			Parent ! {Ref,map_reduce:group(lists:sort(Mapped))}
%% 		end).

%% spawn_reducer(Parent,Reduce,Chunk,Ref) ->
%%     spawn_link(fun() -> Parent ! {Ref,map_reduce:reduce_seq(Reduce,Chunk)} end).

%% map_reduce_dist(Map,M,Reduce,R,Input) ->
%%     Parent = self(),
%%     Ref = make_ref(),
%%     W = length(nodes()),
%%     NodeInputs = lists:zip(nodes(),map_reduce:split_into(W,Input)),
%%     [rpc:call(Node,dets,open_file,[web,[{file,"web.dat"}]]) 
%%      || Node <- nodes()],
    
%%     [spawn_mappers_dist(Parent,Map,R,Split,M div W,Ref) 
%%      || Split <- NodeInputs],
%%     Mappeds = 
%% 	[receive {Ref,L} -> L end || _ <- lists:seq(1,(M div W) * W)],
%%     io:format("received all mapped data~n"),
%%     Reducers = 
%% 	[spawn_reducers_dist(Parent,Reduce,I,Mappeds,Ref)
%% 	 || I <- lists:seq(0,R-1)],
%%     Reduceds = 
%% 	[receive {Pid,L} -> L end || Pid <- Reducers],
%%     lists:sort(lists:flatten(Reduceds)).

%% node_spawn_mapper(Parent,Map,R,Split,Ref) ->
%%     spawn_link(fun() ->
%% 			Mapped = [{erlang:phash2(K2,R),{K2,V2}}
%% 				  || {K,V} <- Split,
%% 				     {K2,V2} <- Map(K,V)],
%% 			Parent ! {Ref,map_reduce:group(lists:sort(Mapped))}
%% 		end).

%% node_spawn_reducer(Parent,Reduce,I,Mappeds,Ref) ->
%%     Inputs = [KV
%% 	      || Mapped <- Mappeds,
%% 		 {J,KVs} <- Mapped,
%% 		 I==J,
%% 		 KV <- KVs],
%%     spawn_link(fun() -> Parent ! {Ref,reduce_seq(Reduce,Inputs)} end).



%% spawn_mappers_dist(Parent,Map,R,{Node,NodeInput},N,Ref) ->
%%     Splits = map_reduce:split_into(N,NodeInput),    
%%     io:format("sent work to ~w~n",[Node]),
%%     rpc:call(Node,map_reduce_dist,node_spawn_mappers,[Parent,Map,R,Splits,Ref]).

%% spawn_reducers_dist(Parent,Reduce,,) ->
%%     Splits = map_reduce:split_into(N,NodeInput),
    

%% node_spawn_mappers(Parent,Map,R,Splits,Ref) ->
%%     io:format("spawning map at ~w~n",[node()]),
%%     [node_spawn_mapper(Parent,Map,R,Split,Ref)
%%      || Split <- Splits].

%% node_spawn_reducers(Parent,Reduce,I,Splits,Ref) ->
%%     io:format("spawning reduce at ~w~n", [node()]),
%%     [node_spawn_reducer(Parent,Reduce,I,Split,Ref)
%%      || Split <- Splits].




%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% %% Generic spawn for dist application of function
%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% spawn_funs_dist(Parent,Fun,R,{Node,NodeInput},N,Ref) ->
%%     Splits = map_reduce:split_into(N,NodeInput),    
%%     io:format("sent work to ~w~n",[Node]),
%%     rpc:call(Node,map_reduce_dist,node_spawn_mappers,[Parent,Map,R,Splits,Ref]).

%% node_spawn_funs(Parent,Fun,R,Splits,Ref) ->
%%     io:format("spawning work at ~w~n",[node()]),
%%     [node_spawn_fun(Parent,Fun,R,Split,Ref)
%%      || Split <- Splits].  

%% node_spawn_fun(Parent,Fun,R,Split,Ref) ->
%%     spawn_link(fun() ->
%% 			Processed = [{erlang:phash2(K2,R),{K2,V2}}
%% 				  || {K,V} <- Split,
%% 				     {K2,V2} <- Fun(K,V)],
%% 			Parent ! {Ref,map_reduce:group(lists:sort(Processed))}
%% 		end).
