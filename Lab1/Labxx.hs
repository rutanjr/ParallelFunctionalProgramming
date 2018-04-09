module Main where

import Data.List
import System.Random
import Criterion.Main
import Control.Parallel
import Control.Parallel.Strategies
import Control.Monad.Par hiding (parMap)
--import Control.DeepSeq


-- code borrowed from the Stanford Course 240h (Functional Systems in Haskell)
-- I suspect it comes from Bryan O'Sullivan, author of Criterion

data T a = T !a !Int

-- * Parallel jacknife, calling the strictly parallel map.
parjack :: (([a] -> b) -> [[a]] -> [b]) -> ([a] -> b) -> [a] -> [b]
parjack mapfun f = mapfun f . resamples 500


-- | Parallel construct, using Control.Parallel (par,pseq),
-- just sparks til the end of time.
------------------------------------------------------------------------------
parPseqMap :: (a -> b) -> [a] -> [b]
parPseqMap f [] = []
parPseqMap f (x:xs) = par first $
               pseq rest
                (first : rest)
  where
    first = f x
    --middle = parmap f (take  (length xs `div` 2) xs)
    rest = parPseqMap f xs -- (drop  (length xs `div` 2) xs)

-- | Eval monad parallel map.
------------------------------------------------------------------------------
evalMap :: (a -> b) -> [a] -> [b]
evalMap _ [] = []
evalMap f (x:xs) = runEval $ do
  first <- rpar (f x)
  rest  <- rseq (evalMap f xs)
  return (first : rest)

-- | Par Monad implementation
------------------------------------------------------------------------------
parMonadMapM :: NFData b => (a -> b) -> [a] -> Par [b]
parMonadMapM _ [] = return []
parMonadMapM f xs = do
  is <- sequence . replicate (length xs) $ new 
  let ys = map f xs
  sequence $ map fork $ zipWith put is ys
  mapM get is
  

parMonadMap :: NFData b => (a -> b) -> [a] -> [b]
parMonadMap f = runPar. parMonadMapM f

-- | Strategy implemenation
------------------------------------------------------------------------------
type Strategy' a = a -> ()

parList' :: Strategy' a -> Strategy' [a]
parList' s [] = ()
parList' s (x:xs) = s x `par` parList' s xs

stratMap ::(a -> b) -> [a] -> [b]
stratMap f xs = map f xs `using` parList rwhnf 

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
        , bench "parjack" (nf (parjack parPseqMap mean) rs)
        , bench "evaljack" (nf (parjack evalMap mean) rs)
        , bench "stratjack" (nf (parjack stratMap mean) rs)
        , bench "parMonadjack" (nf (parjack parMonadMap mean) rs)
        ]

