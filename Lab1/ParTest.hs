module ParTest where

import Control.Parallel -- par and pseq (should be in base)

--------------------------------------------------------------------------------

mFib :: Int -> Int
mFib 0 = 0
mFib 1 = 1
mFib n = mFib (n - 1) + mFib (n - 2)

mEuler :: Int -> Int
mEuler n = sum $ map relPrim [1 .. n - 1]
  where relPrim :: Int -> Int
        relPrim n = length $ filter (\x -> gcd x n == 1) [1 .. n - 1]

main = putStrLn $ show (parFibEul 100)

parFibEul n = fib `par` eul `pseq` fib + eul
  where fib = mFib n
        eul = mEuler n

seqFibEul n = mFib n + mEuler n
