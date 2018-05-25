%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% This implements a page rank algorithm using map-reduce
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-module(page_rank).
-compile(export_all).

%% Use map_reduce to count word occurrences

map(Url,ok) ->
    % added for the nodes, did not impact performance
    dets:open_file(web,[{file,"web.dat"}]),
    [{Url,Body}] = dets:lookup(web,Url),
    Urls = crawl:find_urls(Url,Body),
    [{U,1} || U <- Urls].

reduce(Url,Ns) ->
    [{Url,lists:sum(Ns)}].

page_rank() ->
    {ok,web} = dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    map_reduce:map_reduce_seq(fun map/2, fun reduce/2, 
			      [{Url,ok} || Url <- Urls]).

page_rank_par() ->
    dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    map_reduce:map_reduce_par(fun map/2, 32, fun reduce/2, 32, 
			      [{Url,ok} || Url <- Urls]).

page_rank_dist(M,R) ->
    dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    map_reduce_dist:map_reduce_dist(fun map/2, M, fun reduce/2, R, 
			       [{Url,ok} || Url <- Urls]).

%% Redundant decides whether to use the fault tolerant worker pool
page_rank_pool(M,R,N,Redundant) ->
    dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    Pool = case Redundant of
	       false -> fun worker_pool_dist:worker_pool/2;
	       true -> fun worker_pool_fault:worker_pool/2
	   end,
    map_reduce_pool:map_reduce_pool(fun map/2, M, fun reduce/2, R, 
			       [{Url,ok} || Url <- Urls],Pool,N).

%% Used to save the initial web crawl
save_crawl(Ubody) ->
    {ok,File}=dets:open_file("web.dat" ,[]),
    dets:insert(File, Ubody),
    dets:close(File).

