-module(worker_pool_dist).
-export([worker_pool/2]).

%% Takes the functions to be evaluated (Funs) and the number of processes per
%% node (N).
worker_pool(Funs,N) ->
    Pid = self(),
    Ref = make_ref(),
    spawn(fun() -> 

		  Workers = spawn_workers(N),
		  pool(Funs,Ref,Pid,Workers)
	  end),
    receive
	{Ref,Ans} -> Ans
    end.

pool(Funs,Ref,Sender,Workers) ->
    pool(Funs,length(Funs),Ref,Sender,Workers,[]).
pool(_,0,Ref,Sender,Workers,Ys) -> 
    [W ! exit || W <- Workers],
    Sender ! {Ref,Ys};
pool(Funs,N,Ref,Sender,Workers,Ys) ->
    receive
	{work_request,Worker} -> 
	    Funs_left = case Funs of
			    [F|Fs] -> Worker ! {work,F}, Fs;
			    [] -> Worker ! exit, []		       
			end,
            pool(Funs_left,N,Ref,Sender,Workers,Ys);
	{done,Y} -> 
	    pool(Funs,N-1,Ref,Sender,Workers,[Y|Ys])				 
    end.

spawn_workers(N) ->
    Master = self(),
    Nodes = nodes(),
    [spawn(Node,fun() -> worker(Master) end) 
     || _ <- lists:seq(1,N),Node <- Nodes].
    
worker(Master) ->
    Master ! {work_request,self()},
    receive 
	{work,F} -> 
	    Master ! {done,F()},
	    worker(Master);
	exit -> exit(exiting)
    end.
