-module(map_reduce_dist).
-compile(export_all).

map_reduce_dist(Map,M,Reduce,R,Input) ->
    Parent = self(),
    Splits = map_reduce:split_into(M,Input),
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

spawn_reducer(Parent,Reduce,I,Mappeds,Nodes) ->   
    N = length(Nodes),
    Inputs = [KV
	      || Mapped <- Mappeds,
		 {J,KVs} <- Mapped,
		 I==J,
		 KV <- KVs],
    spawn_link(lists:nth(I rem N + 1,Nodes),
	       fun() -> Parent ! {self(),map_reduce:reduce_seq(Reduce,Inputs)} end).
