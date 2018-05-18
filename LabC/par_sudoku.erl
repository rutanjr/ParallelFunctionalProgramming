-module(par_sudoku).
-import(sudoku,[fill/1,guesses/1,guesses_hard/1,hard/1,refine/1,solve_one/1,solve_refined/1,solved/1,valid_solution/1]).
-import(worker_pool,[start_pool/1,start_pool/2]).
-export([par_solve/2,par_solve_one/4,par_solve_refined/4]).

par_solve(M,N) ->
    % io:format("new problem~n"),
    Refined = refine(fill(M)),
    % Pool = start_pool(master,N),
    Pool = start_pool(N),
    Pid = self(),
    Ref = make_ref(),
    spawn(fun() -> start_par_solve(Refined,Pool,Pid,Ref) end),
    Solution = 
	receive 
	    {done,Ref,{solution,NewM}} -> NewM
	end,
    Pool ! {global,{Ref,solution_found}},
    % io:format("solution found~n"),
    Pool ! exit,
    case valid_solution(Solution) of
	true ->
	    Solution;
	false ->
	    exit({invalid_solution,Solution})
    end.

start_par_solve(M,Pool,Pid,Ref) ->
    Pool ! {get_worker,self()},
    % bugg
    receive
	{worker,W} -> 
	    W ! {work,Ref,Pid,par_sudoku,par_solve_refined,[{hard(M),M},Pool,Pid,Ref]}
    end.	    

par_solve_refined({_H,M},Pool,Pid,Ref) ->
    case solved(M) of
	true -> 
	    {solution,M};
	false ->
	    receive
		{Ref,solution_found} ->
		    exit(solution_found)
	    after
		0 -> 
		    par_solve_one(guesses_hard(M),Pool,Pid,Ref)
	    end
    end.

par_solve_one([],_,_,_) ->
    no_solution;
%par_solve_one([M],Pool,Pid,Ref) ->
%    par_solve_refined(M,Pool,Pid,Ref);
par_solve_one([M|Ms],Pool,Pid,Ref) ->
    Pool ! {get_worker,self()},
    receive
	no_worker -> 
	    case par_solve_refined(M,Pool,Pid,Ref) of
	    	{solution,NewM} -> exit({solution,NewM});
	    	_ -> par_solve_one(Ms,Pool,Pid,Ref)
	    end;
	    %% case catch solve_refined(M) of
	    %% 	{'EXIT',_} -> 
	    %% 	    par_solve_one(Ms,Pool,Pid,Ref);
	    %% 	NewM -> {solution,NewM}
	    %% end;
	{worker,W} -> 
	    W ! {work,Ref,Pid,par_sudoku,par_solve_refined,[M,Pool,Pid,Ref]},
	    par_solve_one(Ms,Pool,Pid,Ref)			 
    end.
