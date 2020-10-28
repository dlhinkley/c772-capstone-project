#!/bin/bash

cd jupter

runipy 01-summarization.ipynb

for file in `ls -1 summary/*.ipynb`
do
  echo "========================================================="
  echo "Running $file"
  echo "========================================================="
  runipy $file
done

runipy 02-investigation.ipynb

for file in `ls -1 investigation/*.ipynb`
do
  echo "========================================================="
  echo "Running $file"
  echo "========================================================="
  runipy $file
done


runipy 03-cleaning.ipynb
