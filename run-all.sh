#!/bin/bash

cd jupter

#runipy 01-summarization.ipynb
jupyter nbconvert --to notebook --inplace --execute 01-summarization.ipynb

for file in `ls -1 summary/*.ipynb`
do
  echo "========================================================="
  echo "Running $file"
  echo "========================================================="
  jupyter nbconvert --to notebook --inplace --execute $file
done

jupyter nbconvert --to notebook --inplace --execute 02-investigation.ipynb

for file in `ls -1 investigation/*.ipynb`
do
  echo "========================================================="
  echo "Running $file"
  echo "========================================================="
  jupyter nbconvert --to notebook --inplace --execute $file
done


jupyter nbconvert --to notebook --inplace --execute 03-cleaning.ipynb
