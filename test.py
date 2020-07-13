#--  This program compares the time and memory used by a pandas dataframe and compares them to those of a dask dataframe. The output shows that a dask dataframe will use far less memory and will be loaded in very little time compared to its pandas counterpart. This shows why dask module is used for parallel computing.

import time
import pandas
import dask.dataframe as dd
import os

def panda():
	start = time.time()
	df = pandas.read_csv ("/home/san1234/dask/automated/dasker.csv")
	stop = time.time()
	total = (stop - start)
	print ("Total --> {}".format (total))

def dask():
	start = time.time()	
	df = dd.read_csv ("/home/san1234/dask/automated/dasker.csv")
	df = df.compute()
	stop = time.time()
	total = (stop - start)
	print ("Total --> {}".format (total))

if __name__ == '__main__':
	panda()
	dask()
