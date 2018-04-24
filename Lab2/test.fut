
-- import "/futlib/math"

let s1 = [23,45,-23,44,23,54,23,12,34,54,7,2, 4,67]
let s2 = [-2, 3, 4,57,34, 2, 5,56,56, 3,3,5,77,89]


let max (n : i32) (m : i32) : i32 =
  if n < m then m else n

let abs (n : i32) : i32 =
  if n < 0 then -n else n

let subt ((n : i32), (m : i32)) : i32 =
  n - m

let process (a: []i32) (b : []i32) : i32 =
  reduce max 0 (map abs (map (subt) (zip a b)))

let length (list : []i32) : i32 = 
  reduce (+) 0 (map (\x -> 1) list)

let max' ((n : i32),(i : i32)) ((m : i32), (i' : i32)) : (i32, i32) =
  if n < m then (m, i') else (n, i)

let process_idx (a: []i32) (b : []i32) : (i32, i32) =
  let vals = (map abs (map (subt) (zip a b)))
  let id_vals = zip vals [0..<(length vals)]
  in reduce max' (0,-1) id_vals

let main (a : []i32) (b : []i32) : i32 = process_idx a b