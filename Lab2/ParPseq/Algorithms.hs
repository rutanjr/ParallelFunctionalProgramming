module Main where

import Data.List
import System.Random
import Criterion.Main
import Test.QuickCheck
import Control.Parallel
import Control.DeepSeq

-- * Used to merge the divided lists from mergesort, 
-- this is the conquering step!
merge :: (Ord a, Num a) => [a] -> [a] -> [a]
merge [] ys = ys
merge xs [] = xs
merge (x:xs) (y:ys) 
                | x > y     = y : (merge (x:xs) ys)
                | otherwise = x : (merge xs (y:ys))

-- | Merge sort
-- Divides the list to be sorted into subparts and then simply uses the merge
-- to sort them, I.E. splits the lists down to lists of single elements.
mergeSort :: (Ord a, Num a) => [a] -> [a]
mergeSort []    = [] -- only neccesary since the input argument can be empty.
mergeSort [x]   = [x] -- for optimization purposes, skips one level of recursion
mergeSort list  = merge xs' ys'
    where (xs, ys) = splitAt (length list `div` 2) list
          xs' = mergeSort xs
          ys' = mergeSort ys

-- | Quick sort
-- Splits the list into smaller + bigger elements and sorts them by themselves
-- based on the first element of the input. Then simply puts the sorted smaller
-- first, then the first element followed by the sorted bigger input.
quickSort :: (Ord a, Num a) => [a] -> [a]
quickSort []        = []
quickSort (x:xs)    = quickSort less ++ (x : quickSort rest)
    where
        less = [y | y <- xs, y < x]
        rest = [y | y <- xs, y >= x]


-- | Merge Sort
-- Using the 'par' construction
mergeSortPar :: (Ord a, Num a) => [a] -> [a]
mergeSortPar []     = []
mergeSortPar [x]    = [x]
mergeSortPar (list) = merge xs' $ xs' `par` ys'
  where (xs, ys) = splitAt (length list `div` 2) list
        xs' = mergeSortPar xs
        ys' = mergeSortPar ys

-- | Quick sort
-- Using the 'par' construction
quickSortPar :: (Ord a, Num a) => [a] -> [a]
quickSortPar []     = []
quickSortPar [x]    = [x]
quickSortPar (x:xs) = par rest $ less ++ (x : rest)
    where
        less = quickSortPar [y | y <- xs, y < x]
        rest = quickSortPar [y | y <- xs, y >= x]

qSort :: (Ord a, Num a) => [a] -> [a]
qSort []     = []
qSort (x:xs) =  par less $ pseq rest less ++ (x : rest)
    where
        less = qSort [y | y <- xs, y < x]
        rest = qSort [y | y <- xs, y >= x]

mSort :: (Ord a, Num a) => [a] -> [a]
mSort []   = []
mSort [x]  = [x]
mSort list = par xs' $ pseq ys' merge xs' ys'
  where (xs, ys) = splitAt (length list `div` 2) list
        xs' = mSort xs
        ys' = mSort ys


qSort' :: (Ord a, Num a) => Int -> [a] -> [a]
qSort' _ []     = []
qSort' d (x:xs) =
  if d > 0 then par less' $ pseq rest' less' ++ (x : rest')
  else (quickSort less) ++ (x : quickSort rest)
    where
         less = [y | y <- xs, y < x]
         rest = [y | y <- xs, y >= x]
         less' = qSort less
         rest' = qSort rest

mSort' :: (Ord a, Num a) => Int ->[a] -> [a]
mSort' _ []   = []
mSort' _ [x]  = [x]
mSort' d list =
  if d > 0 then
    par xs' $ pseq ys' merge xs' ys'
  else mergeSort list
    where (xs, ys) = splitAt (length list `div` 2) list
          xs' = mSort xs
          ys' = mSort ys


main = do
  let xs = take 300000 (randoms (mkStdGen 211570155)) :: [Int]
      n = 2500
      d = 1500
  defaultMain
   [ bench "mergeSort" (nf mergeSort xs)
    , bench ("mergeSortPar") (nf mergeSortPar xs)
    , bench ("mSort, with Pseq") (nf mSort xs)
    , bench ("mSort', with Pseq + depth: " ++ show n) (nf (mSort' n) xs)
    ]
    -- [ bench "quickSort" (nf quickSort xs)
    -- , bench ("quickSortPar") (nf quickSortPar xs)
    -- , bench ("qSort, with Pseq") (nf qSort xs)
    -- , bench ("qSort, with Pseq + depth: " ++ show d) (nf (qSort' d) xs)
    -- ] 

