-module(sudoku).
%-include_lib("eqc/include/eqc.hrl").
-import(par_sudoku,[par_solve/2]).
-compile(export_all).

%% %% generators

%% matrix(M,N) ->
%%     vector(M,vector(N,nat())).

%% matrix transpose

transpose([Row]) ->
    [[X] || X <- Row];
transpose([Row|M]) ->
    [[X|Xs] || {X,Xs} <- lists:zip(Row,transpose(M))].

%% prop_transpose() ->
%%     ?FORALL({M,N},{nat(),nat()},
%% 	    ?FORALL(Mat,matrix(M+1,N+1),
%% 		    transpose(transpose(Mat)) == Mat)).

%% map a matrix to a list of 3x3 blocks, each represented by the list
%% of elements in row order

triples([A,B,C|D]) ->
    [[A,B,C]|triples(D)];
triples([]) ->
    [].

blocks(M) ->
    Blocks = [triples(X) || X <- transpose([triples(Row) || Row <- M])],
    lists:append(
      lists:map(fun(X)->
			lists:map(fun lists:append/1, X)
		end,
		Blocks)).

unblocks(M) ->
    lists:map(
      fun lists:append/1,
      transpose(
	lists:map(
	  fun lists:append/1,
	  lists:map(
	    fun(X)->lists:map(fun triples/1,X) end,
	    triples(M))))).

%% prop_blocks() ->
%%     ?FORALL(M,matrix(9,9),
%% 	    unblocks(blocks(M)) == M).

%% decide whether a position is safe

entries(Row) ->
    [X || X <- Row,
	  1 =< X andalso X =< 9].

safe_entries(Row) ->
    Entries = entries(Row),
    lists:sort(Entries) == lists:usort(Entries).

safe_rows(M) ->
    lists:all(fun safe_entries/1,M).

safe(M) ->
    safe_rows(M) andalso
	safe_rows(transpose(M)) andalso
	safe_rows(blocks(M)).

%% fill blank entries with a list of all possible values 1..9

fill(M) ->
    Nine = lists:seq(1,9),
    [[if 1=<X, X=<9 ->
	      X;
	 true ->
	      Nine
      end
      || X <- Row]
     || Row <- M].

% TEMP
unfill(M) ->
    [[if is_list(X) ->
	      0;
	 true ->
	      X
      end
       || X <- Row]  
     || Row <- M].

parmap(Fun,List) ->
    Parent = self(),
    Refs = [make_ref() || _ <- List],
    [spawn_link(fun () -> 
			case catch Fun(X) of
			    {'EXIT',no_solution} -> Parent ! {Ref,no_solution};
			    Y -> Parent ! {Ref,Y}
			end
		end) 
     || {Ref,X} <- lists:zip(Refs,List)] ,
    [receive 
	 {Ref,no_solution} -> error(no_solution);
	 {Ref,Y} -> Y
     end || Ref <- Refs].
				  
%% refine entries which are lists by removing numbers they are known
%% not to be

member(X,XS) when is_list(XS) -> lists:member(X,XS);
member(X,XS) -> X == XS.

to_list(X) when is_list(X) -> X;
to_list(X) -> [X].

from_list([X]) -> X;
from_list(X) -> X.

merge_entries3(M1,M2,M3) ->
    lists:zipwith3(
      fun(XSS,YSS,ZSS) -> 
	      lists:zipwith3(
		fun(XS,YS,ZS) -> 
			from_list([X || X <- to_list(XS), member(X,YS), member(X,ZS)]) end, 
		XSS, YSS, ZSS)
      end,M1,M2,M3).

spawn_chunk(Fun,M,Ref,Parent) ->
    spawn(fun() ->
		  case catch Fun(M) of
		      {'EXIT',no_solution} -> Parent ! {Ref,no_solution};
		      Y -> Parent ! {Ref,Y}
		  end
	  end).

par_refine(M) ->
    Parent = self(),
    Ref = make_ref(),    
    spawn_chunk(fun(X) -> unblocks(refine_rows(blocks(X))) end ,M,Ref,Parent),
    spawn_chunk(fun(X) -> transpose(refine_rows(transpose(X))) end ,M,Ref,Parent),
    spawn_chunk(fun refine_rows/1 ,M,Ref,Parent),
    [M1,M2,M3] = receive_solutions(Ref,3),
    NewM = merge_entries3(M1,M2,M3),
    if M==NewM ->
	    M;
       true ->
	    par_refine(NewM)
    end.

refine(M) ->
    NewM =
	refine_rows(
	  transpose(
	    refine_rows(
	      transpose(
		unblocks(
		  refine_rows(
		    blocks(M))))))),
    if M==NewM ->
	    M;
       true ->
	    refine(NewM)
    end.

refine_rows(M) ->
    lists:map(fun refine_row/1,M).		 
    %parmap(fun refine_row/1,M).

refine_row(Row) ->
    Entries = entries(Row),
    NewRow =
	[if is_list(X) ->
		 case X--Entries of
		     [] ->			 
			 exit(no_solution);
		     [Y] ->
			 Y;
		     NewX ->
			 NewX
		 end;
	    true ->
		 X
	 end
	 || X <- Row],
    NewEntries = entries(NewRow),
    %% check we didn't create a duplicate entry
    case length(lists:usort(NewEntries)) == length(NewEntries) of
	true ->
	    NewRow;
	false ->
	    exit(no_solution)
    end.

is_exit({'EXIT',_}) ->
    true;
is_exit(_) ->
    false.

%% is a puzzle solved?

solved(M) ->
    lists:all(fun solved_row/1,M).

solved_row(Row) ->
    lists:all(fun(X)-> 1=<X andalso X=<9 end, Row).

%% how hard is the puzzle?

hard(M) ->		      
    lists:sum(
      [lists:sum(
	 [if is_list(X) ->
		  length(X);
	     true ->
		  0
	  end
	  || X <- Row])
       || Row <- M]).

%% choose a position {I,J,Guesses} to guess an element, with the
%% fewest possible choices

guess(M) ->
    Nine = lists:seq(1,9),
    {_,I,J,X} =
	lists:min([{length(X),I,J,X}
		   || {I,Row} <- lists:zip(Nine,M),
		      {J,X} <- lists:zip(Nine,Row),
		      is_list(X)]),
    {I,J,X}.

%% given a matrix, guess an element to form a list of possible
%% extended matrices, easiest problem first.

guesses(M) ->
    {I,J,Guesses} = guess(M),
    Ms = [catch refine(update_element(M,I,J,G)) || G <- Guesses],
    SortedGuesses =
	lists:sort(
	  [{hard(NewM),NewM}
	   || NewM <- Ms,
	      not is_exit(NewM)]),
    [G || {_,G} <- SortedGuesses].

guesses_hard(M) ->
    {I,J,Guesses} = guess(M),
    Ms = [catch refine(update_element(M,I,J,G)) || G <- Guesses],
    lists:sort(
      [{hard(NewM),NewM}
       || NewM <- Ms,
	  not is_exit(NewM)]).

par_guesses(M,N) ->
    {I,J,Guesses} = guess(M),
    Parent = self(),
    Ref = make_ref(),
    [spawn(fun() -> 
		   case catch par_solve_refined(refine(update_element(M,I,J,G)),N-1) of
		       {'EXIT',no_solution} -> Parent ! {Ref,no_solution};
		       NewM -> Parent ! {Ref,NewM}
		   end
	   end) 
     || G <- Guesses],
    receive_solution(Ref,lists:sum([1 ||_ <- Guesses])).

update_element(M,I,J,G) ->
    update_nth(I,update_nth(J,G,lists:nth(I,M)),M).

update_nth(I,X,Xs) ->
    {Pre,[_|Post]} = lists:split(I-1,Xs),
    Pre++[X|Post].

%% prop_update() ->
%%     ?FORALL(L,list(int()),
%% 	    ?IMPLIES(L/=[],
%% 		     ?FORALL(I,choose(1,length(L)),
%% 			     update_nth(I,lists:nth(I,L),L) == L))).

%% solve a puzzle

solve(M) ->
    Solution = par_solve_refined(refine(fill(M)),2),
    % Solution = solve_refined(refine(fill(M))),
    case valid_solution(Solution) of
	true ->
	    Solution;
	false ->
	    exit({invalid_solution,Solution})
    end.

solve_refined(M) ->
    case solved(M) of
	true ->
	    M;
	false ->
	    solve_one(guesses(M))
    end.

solve_one([]) ->
    exit(no_solution);
solve_one([M]) ->
    solve_refined(M);
solve_one([M|Ms]) ->
    case catch solve_refined(M) of
	{'EXIT',no_solution} ->
	    solve_one(Ms);
	Solution ->
	    Solution
    end.

par_solve_refined(M,0) -> solve_refined(M);
par_solve_refined(M,N) ->     
    case solved(M) of
	true ->
	    M;
	false ->
	    par_guesses(M,N-1)
    end.

receive_solution(_,0) -> exit(no_solution);
receive_solution(Ref,N) ->
    receive 
	{Ref,no_solution} -> receive_solution(Ref,N-1);
	{Ref,Sol} -> Sol
    end.

receive_solutions(_,0) -> [];
receive_solutions(Ref,N) -> 
    receive
	{Ref,no_solution} -> exit(no_solution);	     
	{Ref,Sol} -> [Sol|receive_solutions(Ref,N-1)]
    end.

 

%% benchmarks

-define(EXECUTIONS,10).

geomean({_,Rs}) ->
    math:sqrt(lists:sum([math:pow(R,2) || {_,R} <- Rs])).    

bm(F) ->
    {T,_} = timer:tc(?MODULE,repeat,[F]),
    T/?EXECUTIONS/1000.

repeat(F) ->
    [F() || _ <- lists:seq(1,?EXECUTIONS)].

benchmarks(Puzzles) ->
    [{Name,bm(fun()-> solve(M) end)} || {Name,M} <- Puzzles].
benchmarks() ->
  {ok,Puzzles} = file:consult("problems.txt"),
  timer:tc(?MODULE,benchmarks,[Puzzles]).    

par_benchmarks(N,Puzzles) ->    
    [{Name,bm(fun()-> par_solve(M,N) end)} || {Name,M} <- Puzzles].
par_benchmarks(N) ->
    {ok,Puzzles} = file:consult("problems.txt"),
    T = timer:tc(?MODULE,par_benchmarks,[N,Puzzles]).
%    master ! exit, T.    

parbenchmarks(Puzzles) ->
    Parent = self(),
    [spawn_link(
       fun () -> 
         Parent ! {Name,bm(fun() -> solve(M) end)}
       end) 
     || {Name,M} <- Puzzles],
    [receive {Name,Sol} -> {Name,Sol} end || {Name,_} <- Puzzles].
    
		      
%% check solutions for validity

valid_rows(M) ->
    lists:all(fun valid_row/1,M).

valid_row(Row) ->
    lists:usort(Row) == lists:seq(1,9).

valid_solution(M) ->
    valid_rows(M) andalso valid_rows(transpose(M)) andalso valid_rows(blocks(M)).

