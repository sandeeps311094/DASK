#--- This program collects various parameters such as OS name, number of system cores using Bash commands, also gives us performance of a pandas dataframe and a Dask dataframe and loads it into a spreadsheet. The headings of the spreadsheet are also created.

import os
import xlutils
import xlwt
from xlutils.copy import copy
from xlrd import *
import openpyxl
from openpyxl.styles import Font

import platform
import time
import subprocess
import pandas
import psutil

import dask
import dask.dataframe as dd
from dask.distributed import Client, progress

#--

def get_os_info():

	tupp = platform.linux_distribution()
	os_name = ""

	for i in tupp:
		os_name = os_name + " " + i 		#-- Parameter_1 --> os_name

	stats = os.stat(FILENAME)
	dataset_size = stats.st_size			#-- Parameter_2 --> dataset_size

	memory_limit = subprocess.getoutput('cat /sys/fs/cgroup/memory/memory.limit_in_bytes')
											#-- Parameter_3 --> memory_limit
	
	number_of_processors = subprocess.getoutput("cat /proc/cpuinfo | awk '/^processor/{print $3}' | wc -l")
											#-- Parameter_4 --> number_of_processors

	ls_os = [os_name, dataset_size, memory_limit, number_of_processors]

	return (ls_os)

#--

def pandas1():
	start = time.time()

	dframe = pandas.read_csv("dasker_low.csv")
	#pdb.set_trace()

	stop = time.time()

	total_time_pd = (stop - start)			#-- Parameter_5 --> total_time_pd (Pandas_time)

	pid = os.getpid()
	ps = psutil.Process(pid)
	memo = ps.memory_info()				

	memo_MB_pd = (memo.rss) / (1000000)		#-- Parameter_6 --> memo_MB_pd (Pandas_memory)

	ls_pandas = [total_time_pd, memo_MB_pd]

	return (ls_pandas)

	#return (memo.rss, memo.vms)
	
#--

def dask1():
	start = time.time()

	dframe1 = dd.read_csv('dasker_low.csv')
	#pdb.set_trace()
	
	stop = time.time()

	total_time_dsk = (stop - start)				#-- Parameter_7 --> total_time_dsk (DASK_time)

	pid = os.getpid()
	ps = psutil.Process(pid)
	memo = ps.memory_info()	

	memo_MB_dsk = (memo.rss) / (1000000)		#-- Parameter_8 --> memo_MB_dsk (DASK_memory)		

	ls_dask = [total_time_dsk, memo_MB_dsk]

	return (ls_dask)

	#return (memo.rss, memo.vms)

#--

def form_list():
	ls_1 = get_os_info()
	ls_2 = pandas1()
	ls_3 = dask1()
	
	ls_fin = ls_1 + ls_2 + ls_3
	return (ls_fin)

#--

def get_i():
	fob = open("i_holder.txt", "r+")
	x = fob.read()

	x = int(x)
	y = str(x + 1)
	i = x

	fob = open("i_holder.txt", "w+")
	fob.write(y)

	fob.close()

	return (i)

#--

def master():
	ls_fin = form_list()
	i = get_i()
	
	xlsx_write(ls_fin, i)

#--

def xlsx_write (ls_fin, i):
	
	ls = ls_fin
	print (i)
	w = copy(open_workbook('book1t.xlsx'))
	#wb = openpyxl.Workbook()
	#sheet = wb.active
	
	j = 0

	while (j <= 7):
		temp = ls[0]
		w.get_sheet(0).write(i, j, temp)
		w.save ('book1t.xlsx')

		j += 1
		ls.remove(ls[0])
	
	#wb.save ('book1t.xlsx')
	w.save ('book1t.xlsx')

#--

def re_initialize_i_txt():
	i = str (2)
	fob = open ("i_holder.txt", "w+")

	fob.write (i)
	fob.close()

#--

def organize_worksheet():
	wb = openpyxl.Workbook()
	page = wb.active
	ls = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']

	for column in ls:
		page.column_dimensions [column].width = 30

	wb.save ('book1t.xlsx')	

#--

def test_master():
	pass

#--

def heading():
	j = 0
	w = copy(open_workbook('book1t.xlsx'))
	list_headings = ['Operating System', 'Data Size', 'Base Memory', 'Number of Processors', 'Time taken by Pandas', 'Memory used by Pandas', 'Time taken by DASK', 'Memory used by DASK']

	while (j <= 7):
		temp = list_headings[0]

		w.get_sheet(0).write(0, j, temp)
		w.save ('book1t.xlsx') = 

		j += 1
		list_headings.remove(list_headings[0])

	w.save ('book1t.xlsx')

#--

FILENAME = "/home/san1234/Calligo/dasker_low.csv"

if __name__ == '__main__':
	
	#organize_worksheet()

	heading()
	master()

	
	#re_initialize_i_txt()
	#reinitalize_i_txt()


