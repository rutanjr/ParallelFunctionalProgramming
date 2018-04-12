module Main where

import Data.List
import System.Random
import Criterion.Main
import Test.QuickCheck
import Control.Monad (replicateM)
import Control.Parallel
import Control.Parallel.Strategies
import Control.Monad.Par

-- * Assigment 2
------------------------------------------------------------------------------

-- | Merge of two sorted lists
merge :: Ord a => [a] -> [a] -> [a]
merge [] ys = ys
merge xs [] = xs
merge (x:xs) (y:ys) =
  if x <= y
  then x : merge xs (y:ys)
  else y : merge (x:xs) ys

-- | Sequential merge sort.
mergeSort :: Ord a => [a] -> [a]
mergeSort xs | length xs < 2 = xs
             | otherwise     = merge (mergeSort ys) (mergeSort zs)
  where
    (ys,zs) = splitAt (length xs `div` 2) xs

-- | Checks if a list if sorted 
isSorted :: Ord a => [a] -> Bool
isSorted [] = True
isSorted xs = and $ zipWith (<=) (init xs) (tail xs)

-- | Property that mergeSort sorts its input.
prop_mergeSorted :: Ord a => [a] -> Bool
prop_mergeSorted = isSorted . mergeSort

parMergeSortPar :: (NFData a, Ord a) => Int -> [a] -> [a]
parMergeSortPar n xs | length xs < n = mergeSort xs
                     | otherwise     = runPar $ do
  let (ys,zs) = splitAt (length xs `div` 2) xs
  i <- spawn $ return $ parMergeSortPar n ys
  j <- spawn $ return $ parMergeSortPar n zs
  ys' <- get i
  zs' <- get j
  return $ merge ys' zs'

parMergeSortEval :: (NFData a, Ord a) => [a] -> [a]
parMergeSortEval xs = runEval $ do
  let (ys,zs) = splitAt (length xs `div` 2) xs
  ys' <- rpar (parMergeSortEval ys)
  zs' <- rpar (parMergeSortEval zs)
  rseq ys'
  rseq zs'
  return $ merge ys' zs'

parMergeSortGran :: (NFData a, Ord a) => Int -> ([a] -> [a]) -> [a] -> [a]
parMergeSortGran n psort xs | length xs < n = mergeSort xs
                            | otherwise     = psort xs

-- * Main method for testing
------------------------------------------------------------------------------

main = do
  let xs = (take 6000 (randoms (mkStdGen 211570155)) :: [Float] )
  defaultMain
    [ bench "sequential" (nf mergeSort xs)
    , bench "par monad" (nf (parMergeSortPar 1000) xs)
    ]
