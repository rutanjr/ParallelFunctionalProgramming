-module(map_reduce_pool).
-export([map_reduce_pool/7]).

%% N is the number of processes per node
map_reduce_pool(Map,M,Reduce,R,Input,Pool,N) ->
    Splits = map_reduce:split_into(M,Input),
    Maps = [mapper(Map,R,Split) || Split <- Splits],
    Mappeds = Pool(Maps,N), %worker_pool_dist:worker_pool(Maps,N),
    Reduces = [reducer(Reduce,I,Mappeds)
	       || I <- lists:seq(0,R-1)],
    Reduceds = Pool(Reduces,N), % worker_pool_dist:worker_pool(Reduces,N),
    lists:sort(lists:flatten(Reduceds)).

mapper(Map,R,Split) ->
    fun() ->
	    Mapped = [{erlang:phash2(K2,R),{K2,V2}}
		      || {K,V} <- Split,
			 {K2,V2} <- Map(K,V)],
	    map_reduce:group(lists:sort(Mapped))
    end.
reducer(Reduce,I,Mappeds) ->
    Inputs = [KV
	      || Mapped <- Mappeds,
		 {J,KVs} <- Mapped,
		 I==J,
		 KV <- KVs],
    fun() -> 
	    map_reduce:reduce_seq(Reduce,Inputs)
    end.
