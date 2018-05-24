-module(worker_pool_fault).
-export([test/0,worker_pool/2]).

%% Takes the functions to be evaluated (Funs) and the number of processes per
%% node (N).
worker_pool(Funs,N) ->
    Pid = self(),
    Ref = make_ref(),
    spawn(fun() -> 
		  process_flag(trap_exit,true),
		  Workers = spawn_workers(N),
		  io:format("workers_spawned~n~w~n",[Workers]),
		  pool(Funs,Ref,Pid,Workers)
	  end),
    io:format("waiting for response~n"),
    receive
	{Ref,Ans} -> Ans
    end.

pool(Funs,Ref,Sender,Workers) ->
    pool(Funs,[],length(Funs),Ref,Sender,Workers,[]).
pool(_,_,0,Ref,Sender,Workers,Ys) -> 
    [W ! exit || W <- Workers],
    Sender ! {Ref,Ys};
pool(Funs,Active,N,Ref,Sender,Workers,Ys) ->
    receive
	{'EXIT',_,normal} ->
	    pool(Funs,Active,N,Ref,Sender,Workers,Ys);
	{'EXIT',Worker,_} ->
	    io:format("CRASH: ~w",[Worker]),
	    Workers_new = lists:delete(Worker,Workers),
	    case lists:keytake(Worker,1,Active) of
		false ->
		    pool(Funs,Active,N,Ref,Sender,Workers_new,Ys);
		{value,{_,Fun},Active_new} ->
		    pool([Fun|Funs],Active_new,N,Ref,Sender,Workers_new,Ys)
	    end;
	{work_request,Worker} -> 
	    case Funs of
		[F|Fs] -> 
		    Worker ! {work,F}, 
		    pool(Fs,[{Worker,F}|Active],N,Ref,Sender,Workers,Ys);
		[] -> 
		    %% FIX REDUNDANCY
		    % Worker ! exit, 		       
		    pool([],Active,N,Ref,Sender,Workers,Ys)
	    end;
	{done,Worker,Y} -> 
	    Active_new = lists:keydelete(Worker,1,Active),
	    pool(Funs,Active_new,N-1,Ref,Sender,Workers,[Y|Ys])
    end.

spawn_workers(N) ->
    Master = self(),
    Nodes = nodes(),
    [spawn_link(Node,fun() -> worker(Master) end) 
     || _ <- lists:seq(1,N),Node <- Nodes].
    
worker(Master) ->
    Master ! {work_request,self()},
    receive 
	{work,F} -> 
	    Master ! {done,self(),F()},
	    worker(Master);
	exit -> exit
    end.

%% Testing
test() ->
    N = 4,
    M = 1,
    io:format("making function list~n"),
    Funs = [fun() -> timer:sleep(5000), {self(),node()} end || _ <- lists:seq(1,N)],
    timer:tc(?MODULE,worker_pool,[Funs,M]).

    
