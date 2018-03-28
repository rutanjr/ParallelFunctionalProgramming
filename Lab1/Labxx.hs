module Main where

import Data.List
import System.Random
import Criterion.Main
import Control.Parallel

-- code borrowed from the Stanford Course 240h (Functional Systems in Haskell)
-- I suspect it comes from Bryan O'Sullivan, author of Criterion

data T a = T !a !Int

-- | Parallel constructs working in a certain depth.
------------------------------------------------------------------------------


-- * jacknife calling depth par map.
depthparjack :: Int -> ([a] -> b) ->  [a] -> [b]
depthparjack d f = (depthparmap d f) . resamples 500

-- * Depth parametrized map.
depthparmap :: Int -> (a -> b) ->  [a] -> [b]
depthparmap _ _ [] = []
depthparmap d f (x : xs) =
  if d > 0 then par middle $ pseq rest first ++ middle ++ rest
           else [f x] ++ map f xs
     where first = [f x]
           middle = parmap f (take  (length xs `div` 2) xs)
           rest = parmap f (drop  (length xs `div` 2) xs)
           
-- | Parallel constructs, just sparks til the end of time.
------------------------------------------------------------------------------

-- * Parallel jacknife, calling the strictly parallel map.
parjack :: ([a] -> b) -> [a] -> [b]
parjack f = parmap f . resamples 500


-- * Parallel map. Sparks everywhere.
parmap :: (a -> b) -> [a] -> [b]
parmap f [] = []
parmap f (x:xs) = par middle $
               pseq rest
                first ++ middle ++ rest
  where
    first = [f x]
    middle = parmap f (take  (length xs `div` 2) xs)
    rest = parmap f (drop  (length xs `div` 2) xs)


mean :: (RealFrac a) => [a] -> a
mean = fini . foldl' go (T 0 0)
  where
    fini (T a _) = a
    go (T m n) x = T m' n'
      where m' = m + (x - m) / fromIntegral n'
            n' = n + 1


resamples :: Int -> [a] -> [[a]]
resamples k xs =
    take (length xs - k) $
    zipWith (++) (inits xs) (map (drop k) (tails xs))


jackknife :: ([a] -> b) -> [a] -> [b]
jackknife f = map f . resamples 500


crud = zipWith (\x a -> sin (x / 300)**2 + a) [0..]

main = do
  let (xs,ys) = splitAt 1500  (take 6000
                               (randoms (mkStdGen 211570155)) :: [Float] )
  -- handy (later) to give same input different parallel functions

  let rs = crud xs ++ ys
  putStrLn $ "sample mean:    " ++ show (mean rs)

  let j = jackknife mean rs :: [Float]
  putStrLn $ "jack mean min:  " ++ show (minimum j)
  putStrLn $ "jack mean max:  " ++ show (maximum j)
  defaultMain
        [ bench "jackknife" (nf (jackknife  mean) rs)
        , bench "parjack" (nf (parjack  mean) rs)
        , bench "depthparjack" (nf (depthparjack 2 mean) rs)
        ]

