-module(par_sudoku).
-import(sudoku,[fill/1,guess/1,guesses/1,refine/1,solve_one/1,solve_refined/1,solved/1,valid_solution/1]).
-import(worker_pool,[start_pool/1,start_pool/2]).
-export([par_solve/2,par_solve_one/4,par_solve_refined/4]).

par_solve(M,N) ->
    % io:format("new problem~n"),
    Refined = refine(fill(M)),
    Pool = start_pool(master,N),
    Pid = self(),
    Ref = make_ref(),
    spawn(fun() -> start_par_solve(Refined,Pool,Pid,Ref) end),
    Solution = 
	receive 
	    {done,Ref,{solution,NewM}} -> NewM
	end,
    Pool ! {global,{Ref,solution_found}},
    % io:format("solution found~n"),
    % Pool ! exit,
    case valid_solution(Solution) of
	true ->
	    Solution;
	false ->
	    exit({invalid_solution,Solution})
    end.

start_par_solve(M,Pool,Pid,Ref) ->
    Pool ! {get_worker,self()},
    receive
	{worker,W} -> 
	    W ! {work,Ref,Pid,par_sudoku,par_solve_refined,[M,Pool,Pid,Ref]}
    end.	    

par_solve_refined(M,Pool,Pid,Ref) ->
    case solved(M) of
	true -> 
	    {solution,M};
	false ->
	    par_solve_one(guesses(M),Pool,Pid,Ref)
    end.

par_solve_one([],_,_,_) ->
    no_solution;
par_solve_one([M],Pool,Pid,Ref) ->
    receive 
	{Ref,solution_found} -> 
	    solution_found
    after 
	0 -> par_solve_refined(M,Pool,Pid,Ref)
    end;	
par_solve_one([M|Ms],Pool,Pid,Ref) ->
    receive
	{Ref,solution_found} -> 
	    {Ref,solution_found}
    after
	0 -> 
	    Pool ! {get_worker,self()},
	    receive
		no_worker -> 
		    case catch solve_one([M|Ms]) of
			{'EXIT',_} -> no_solution;
			NewM -> {solution,NewM}
		    %% case catch solve_one_check([M|Ms],Pool,Pid,Ref)of
		    %% 	{'EXIT',_} -> 
		    %% 	    no_solution;
		    %% 	{solution,NewM} -> 
		    %% 	    {solution,NewM};
		    %% 	NewM -> NewM
			       
		    end;
		{worker,W} -> 
		    W ! {work,Ref,Pid,par_sudoku,par_solve_refined,[M,Pool,Pid,Ref]},
		    par_solve_one(Ms,Pool,Pid,Ref)			 
	     end
    end.

solve_one_check([],_,_,_) ->
    exit(no_solution);
solve_one_check([M],Pool,Pid,Ref) ->
    receive
	{Ref,solution_found} -> {Ref,solution_found}
    after
	0 -> 
	    %{solution,solve_refined(M)}
	    par_solve_refined([M],Pool,Pid,Ref)
    end;
solve_one_check([M|Ms],Pool,Pid,Ref) ->
    receive
	{Ref,solution_found} -> {Ref,solution_found}
    after
	0 -> 
	    case catch solve_refined(M) of
		{'EXIT',no_solution} ->
		    par_solve_one(Ms,Pool,Pid,Ref);
		NewM ->
		    {solution,NewM}
	    end
    end.
