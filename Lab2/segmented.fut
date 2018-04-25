let segscan [n] 't (op: t -> t -> t) (ne: t) (arr: [n](t, bool)): [n]t =
  let op' = \(t1,f1) (t2,f2) -> (if f2 then t2 else op t1 t2, or [f1,f2])
  in map (\(t,_) -> t) (scan op' (ne,false) arr)

let main [n] (xs: [n]i32): [n]i32 =
  segscan (*) 1 (zip xs (map (\x -> x%5 == 0) (iota n)))