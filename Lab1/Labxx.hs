module Main where

import Data.List
import System.Random
import Criterion.Main
import Control.Parallel
import Control.Parallel.Strategies
import Control.Monad.Par

-- code borrowed from the Stanford Course 240h (Functional Systems in Haskell)
-- I suspect it comes from Bryan O'Sullivan, author of Criterion

data T a = T !a !Int

-- | Parallel jackknife taking a parallel mapping function as an extra
-- argument.
parJack :: (([a] -> b) -> [[a]] -> [b]) -> ([a] -> b) -> [a] -> [b]
parJack mapf f = mapf f . resamples 500

-- | Parallel construct, using Control.Parallel (par,pseq),
-- just sparks til the end of time.
parMapPseq :: (a -> b) -> [a] -> [b]
parMapPseq f []     = []
parMapPseq f (x:xs) = y `par` ys `pseq` y:ys
  where
    y = f x
    ys = parMapPseq f xs

-- | Parallel map utilizing the Eval monad.
parMapEval :: (a -> b) -> [a] -> [b]
parMapEval _ []     = []
parMapEval f (x:xs) = runEval $ do
  y  <- rpar (f x)
  ys <- rseq (parMapEval f xs)
  return $ y:ys

-- | Implementation of a monadic parallel map using the Par monad. The result
-- is still wrapped inside tha Par monad.
parMapParM :: NFData b => (a -> b) -> [a] -> Par [b]
parMapParM _ [] = return []
parMapParM f xs = do
  is <- sequence . replicate (length xs) $ new 
  let ys = map f xs
  sequence $ map fork $ zipWith put is ys
  mapM get is

-- | Implementation of map using the Par monad.  
parMapPar :: NFData b => (a -> b) -> [a] -> [b]
parMapPar f = runPar. parMapParM f

-- | Strategy implemenation
------------------------------------------------------------------------------
type Strategy' a = a -> ()

parList' :: Strategy' a -> Strategy' [a]
parList' s [] = ()
parList' s (x:xs) = s x `par` parList' s xs

parMapStrat ::(a -> b) -> [a] -> [b]
parMapStrat f xs = map f xs `using` parList rwhnf 

-- * Predefined funtions
------------------------------------------------------------------------------
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

-- * Implementation of different test cases in main
------------------------------------------------------------------------------

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
        [ bench "sequential" (nf (jackknife           mean) rs)
        , bench "par, pseq"  (nf (parJack parMapPseq  mean) rs)
        , bench "Eval monad" (nf (parJack parMapEval  mean) rs)
        , bench "strategies" (nf (parJack parMapStrat mean) rs)
        , bench "Par monad"  (nf (parJack parMapPar   mean) rs)
        ]

