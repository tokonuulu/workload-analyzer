#!/usr/local/bin/python 
import sys 
from collections import defaultdict


class Request:
	time = 0.0
	dev =  0
	blk_start = 0
	blk_end = 0
	blk_count = 0
	is_read = True

	def __init__(self, time, dev, blk_start, blk_count, is_read):
		self.time = time
		self.dev = dev
		self.blk_start = blk_start
		self.blk_end = blk_start + blk_count-1
		self.blk_count = blk_count
		self.is_read = is_read

	def is_seq(self, prev_req, **kwargs):
		ignore_dev = kwargs.get('ignore_dev', False)
		ignore_rw = kwargs.get('ignore_rw', False)
		overlap = kwargs.get('overlap', True)
		if ignore_dev == False:
			if self.dev != prev_req.dev:
				return False

		if ignore_rw == False:
			if self.is_read != prev_req.is_read:
				return False
		if overlap:
			if self.blk_start <= prev_req.blk_end + 1 and self.blk_start >= prev_req.blk_start:
				return True
		else:
			if self.blk_start == prev_req.blk_end + 1:
				return True
		return False

def parse_trace_disksim(s):

	w=s.split()	
	arrivetime = float(w[0])
	devno =  int(w[1])
	blkno = int(w[2])
	bcount = int(w[3])
	if int(w[4]) == 1:
		readflag = True
	else:
		readflag = False

	return arrivetime, devno, blkno, bcount, readflag


def load_trace(filename):
	workloads = []
	try: 
		file = open(filename)
		for s in file:
		
			arrivetime, devno, blkno, bcount, readflag = parse_trace_disksim(s)
			req = Request(arrivetime, devno, blkno, bcount, readflag)
			workloads.append(req)
		file.close()

	except IOError:
		print >> sys.stderr, " Cannot open file "

	return workloads

def analysis_trace(workloads, **kwargs):
	if len(workloads) < 2:
		print "len(workloads) < 2"

	i = 1
	read_cnt = 0
	read_size = 0
	write_cnt = 0
	write_size = 0
	total_cnt = 0
	total_size = 0
	merged_cnt = 0
	merged_read_cnt = 0
	merged_write_cnt = 0

	req_first = workloads[0]
	total_cnt += 1
	total_size = req_first.blk_count 
	if req_first.is_read:
		read_cnt += 1
		read_size = req_first.blk_count
	else:
		write_cnt += 1
		write_cnt = req_first.blk_count

	while(i < len(workloads)):
		prev_req = workloads[i-1]
		curr_req = workloads[i]

		total_cnt += 1
		total_size += curr_req.blk_count
		if curr_req.is_read == True:
			read_cnt += 1
			read_size += curr_req.blk_count
		else:
			write_cnt += 1
			write_size += curr_req.blk_count
		
		if curr_req.is_seq(prev_req, **kwargs) == False:
			merged_cnt += 1
			if curr_req.is_read == True:
				merged_read_cnt += 1
			else:
				merged_write_cnt += 1

		i = i+1

	total_size = float(total_size) * 512 / 1024
	read_size = float(read_size) * 512 / 1024
	write_size = float(write_size) * 512 / 1024

	avg_size = float(total_size) / merged_cnt
	avg_read_size = float(read_size) / merged_read_cnt
	avg_write_size = float(write_size) / merged_write_cnt

	print "Total Requset Count : %d" % total_cnt
	print "Total Request Size : %d KB" % total_size
	print "Read Requset Count : %d" % read_cnt
	print "Read Request Size : %d KB" % read_size
	print "Write Requset Count : %d" % write_cnt
	print "Write Request Size : %d KB" % write_size
	print
	print "Merged Request Count : %d" % merged_cnt
	print "Merged Read Request Count : %d" % merged_read_cnt
	print "Merged Write Request Count : %d" % merged_write_cnt
	print
	print "Average Sector Size : %.1f KB" % avg_size
	print "Average Read Sector Size : %.1f KB" % avg_read_size
	print "Average Write Sector Size : %.1f KB" % avg_write_size
			
# main program 
if len(sys.argv) != 2 :
	print sys.stderr, " Invalid args "  
	exit(1)

print "Trace File = ", sys.argv[1]
file_trace = sys.argv[1] 
print "load_trace ..."
workloads = load_trace(file_trace)
print "load_trace complete !!"
print "Analysis_trace"
print
print
print "================== Result ====================="
analysis_trace(workloads, overlap=True, ignore_rw=False, ignore_dev=False)
