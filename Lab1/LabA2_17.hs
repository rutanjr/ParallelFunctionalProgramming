module Main where

import Data.List
import System.Random
import Criterion.Main
import Test.QuickCheck
import Control.Monad (replicateM)
import Control.Parallel
import Control.Parallel.Strategies
import Control.Monad.Par
import Control.DeepSeq

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

parMergeSortEval :: (NFData a, Ord a) => Int -> [a] -> [a]
parMergeSortEval n xs | length xs < n = mergeSort xs
                      | otherwise     = runEval $ do
  let (ys,zs) = splitAt (length xs `div` 2) xs
  ys' <- rpar $ force (parMergeSortEval n ys)
  zs' <- rseq $ force (parMergeSortEval n zs)
  return $ merge ys' zs'

parMergeSortGran :: (NFData a, Ord a) => Int -> ([a] -> [a]) -> [a] -> [a]
parMergeSortGran n psort xs | length xs < n = mergeSort xs
                            | otherwise     = psort xs

-- * Main method for testing
------------------------------------------------------------------------------

main = do
  let xs = (take 100000 (randoms (mkStdGen 211570155)) :: [Float] )
      n = 5000  
  defaultMain
    [ bench "Sequential" (nf mergeSort xs)
    , bench ("ParMonad " ++ show n) (nf (parMergeSortPar n) xs)
    , bench ("EvalMonad " ++ show n) (nf (parMergeSortEval n) xs)
    ]
