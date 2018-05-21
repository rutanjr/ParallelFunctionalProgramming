-module(worker_pool).
-export([start_pool/1]).

start_pool(N) ->    
    spawn(fun() ->
		  Pid = self(),
		  Workers = [spawn(fun() -> worker(Pid) end) || _ <- lists:seq(1,N)],
		  pool(Pid,N,Workers,Workers)
	  end).
worker(Pid) ->
    receive 
	{work,Ref,Receiver,Mod,Fun,Args} ->
	    Ans = case catch apply(Mod,Fun,Args) of
		{'EXIT',Msg} -> Msg;
		X -> X
	    end,
	    Receiver ! {done,Ref,Ans},
	    Pid ! {done,self()},
	    worker(Pid);
	exit -> exit
    end.

pool(Pid,N,Workers,All) ->
    receive
	{done, W} -> 
	    pool(Pid,N+1,[W|Workers],All);
	{get_worker,Sender} ->
	    case Workers of
		[] -> 
		    Sender ! no_worker, 
		    pool(Pid,N,[],All);
		[W|Ws] -> 
		    Sender ! {worker,W}, 
		    pool(Pid,N-1,Ws,All)			 
	    end;
	{global, Msg} -> 
	    [W ! Msg || W <- Workers], 
	    pool(Pid,N,Workers,All);
	exit -> 
	    [W ! exit || W <- All], 
	    exit(shutting_down_pool);
	print -> 
	    io:format("~w has ~w workers~n",[Pid,N]),
	    pool(Pid,N,Workers,All)
    end.
