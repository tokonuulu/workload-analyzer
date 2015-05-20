#!/usr/local/bin/python 
import sys 
from collections import defaultdict
import bisect

def str2bool(v):
	v = str(v)
	return v.lower() in ("yes", "true", "t", "1")

class Request:
	time = 0.0
	dev =  0
	blk_start = 0
	blk_end = 0
	blk_count = 0
	is_read = True
	back_seq_req = None
	front_seq_req = None

	def __init__(self, time, dev, blk_start, blk_count, is_read):
		self.time = time
		self.dev = dev
		self.blk_start = blk_start
		self.blk_end = blk_start + blk_count-1
		self.blk_count = blk_count
		self.is_read = is_read

	def merge(self, next_req):
		self.time = min(self.time, next_req.time)
		self.blk_start = min(self.blk_start, next_req.blk_start)
		self.blk_end = max(self.blk_end, next_req.blk_end)
		self.blk_count += next_req.blk_count

	def is_seq(self, prev_req, **kwargs):
		ignore_dev = str2bool(kwargs.get('ignore_dev', False))
		ignore_rw = str2bool(kwargs.get('ignore_rw', False))
		overlap = str2bool(kwargs.get('overlap', True))
		interval_time = float(kwargs.get('interval_time', -1.0))

		if ignore_dev == False:
			if self.dev != prev_req.dev:
				return False

		if ignore_rw == False:
			if self.is_read != prev_req.is_read:
				return False

		if interval_time >= 0:
			if abs(self.time - prev_req.time) > interval_time:
				return False

		if overlap:
			if self.blk_start <= prev_req.blk_end + 1 and self.blk_end >= prev_req.blk_end:
				return True
			elif self.blk_start >= prev_req.blk_start and self.blk_end <= prev_req.blk_end:
				return True
			elif self.blk_start <= prev_req.blk_start and self.blk_end >= prev_req.blk_end:
				return True
			
		else:
			if self.blk_start == prev_req.blk_end + 1:
				return True
		return False

def parse_trace(s, format):
	if format == "disksim":
		w=s.split()	
		arrivetime = float(w[0])
		devno =  int(w[1])
		blkno = int(w[2])
		bcount = int(w[3])
		if int(w[4]) == 1:
			readflag = True
		else:
			readflag = False

	elif format == "blktrace":
		w=s.split()	
		arrivetime = float(w[0])
		devno =  0
		blkno = int(w[2])
		bcount = int(w[3])
		if 'R' in str(w[1]):
			readflag = True
		elif 'W' in str(w[1]):
			readflag = False
		else:
			return None

	return arrivetime, devno, blkno, bcount, readflag

def print_workloads(workloads):
	for req in workloads:
		print req.time, req.blk_start, req.blk_end, req.blk_count, req.is_read

def load_trace(filename, format, **kwargs):
	load_read = str2bool(kwargs.get('load_read', True))
	load_write = str2bool(kwargs.get('load_write', True))

	workloads = []
	trace = []
	print "Trace File = ", file_name, " Format=", file_format

	try: 
		print "load_trace ..."
		file = open(filename)
		for s in file:
			if "CPU" in s:
				break

			tmp = parse_trace(s,format)
			if tmp == None:
				continue
			arrivetime, devno, blkno, bcount, readflag = tmp
			if load_read == False and readflag == True:
				continue
			elif load_write == False and readflag == False:
				continue

			req = Request(arrivetime, devno, blkno, bcount, readflag)
			workloads.append(req)
			#insert_req(workloads, req)
		file.close()
		workloads.sort(key = lambda x: x.time)

		file = open(filename+".sorted","w+")
		for req in workloads:
			file.write(str(req.time)+"\n")
		file.close()
	except IOError:
		print >> sys.stderr, " Cannot open file "

	return workloads

# def insert_req(workloads, new_req):
# 	for req in reversed(workloads):
# 		if req.time < new_req.time:
# 			idx = workloads.index(req) + 1
# 			workloads.insert(idx, new_req)
# 			return
# 	workloads.insert(0,new_req)

# def merge_seq(ori_workloads, **kwargs):
# 	merged_workloads = []
# 	interval_time = kwargs.get('interval_time', -1.0)
# 	a=0
# 	while ori_workloads:
# 		a+=1
# 		if a%100 == 0:
# 			print a
# 		curr_req = ori_workloads.pop(0)

# 		# i=len(merged_workloads) - 1
# 		# while i >= 0 :
# 		# 	prev_req = merged_workloads[i]
# 		# 	if abs(prev_req.time - curr_req.time) > interval_time:
# 		# 		break

# 		# 	is_seq = curr_req.is_seq(prev_req, **kwargs) or prev_req.is_seq(curr_req, **kwargs)
# 		# 	if is_seq:
# 		# 		curr_req.merge(prev_req)
# 		# 		merged_workloads.remove(prev_req)
# 		# 		break
# 		# 	break
# 		#merged_workloads.append(curr_req)
# 		#insert_req(merged_workloads,curr_req)
			
# 	return merged_workloads

# def insert_req(workloads, new_req):
# 	for req in reversed(workloads):
# 		if req.time < new_req.time:
# 			idx = workloads.index(req) + 1
# 			workloads.insert(idx, new_req)
# 			return
# 	workloads.insert(0,new_req)


def analysis_trace(workloads, **kwargs):


	if len(workloads) < 2:
		print "len(workloads) < 2"
		return
	interval_time = float(kwargs.get('interval_time', -1.0))
	read_cnt = 0
	read_size = 0
	write_cnt = 0
	write_size = 0
	total_cnt = 0
	total_size = 0
	merged_cnt = 0
	merged_read_cnt = 0
	merged_write_cnt = 0

	total =len(workloads)
	i = 0
	while(i < len(workloads)):
		if i%200 == 0:
			sys.stdout.write("\rComplete: %.2f%% " % (float(i)*100 / total))
			sys.stdout.flush()
		#print "\r %.2f%% Complete " % (float(i)*100 / total)
		curr_req = workloads[i]

		total_cnt += 1
		total_size += curr_req.blk_count
		merged_cnt += 1

		if curr_req.is_read == True:
			read_cnt += 1
			read_size += curr_req.blk_count
			merged_read_cnt += 1
		else:
			write_cnt += 1
			write_size += curr_req.blk_count
			merged_write_cnt += 1

		j = 1

		front_seq = False
		back_seq = False
		front_dup = False
		back_dup = False

		while( True ):
			if i - j < 0:
				break
			if i > 0:
				prev_req = workloads[i-j]

				if interval_time >= 0:
					if abs(curr_req.time-prev_req.time) > interval_time :
						break
					# else:
					# 	print "Not Sequential : ", curr_req.time

				if front_seq == False:
					front_seq = curr_req.is_seq(prev_req, **kwargs)
					if front_seq == True:
						if prev_req.back_seq_req != None:
							#print "front ", prev_req.back_seq_req.time , prev_req.time , curr_req.time 
							front_dup = True
						#else:
						#	print "front connect ", prev_req.time , curr_req.time 
						curr_req.front_seq_req = prev_req
						prev_req.back_seq_req = curr_req

						
				
				if back_seq == False:
					back_seq = prev_req.is_seq(curr_req, **kwargs)

					if back_seq == True:
						if prev_req.front_seq_req != None:
							#print "back ", prev_req.front_seq_req.time , prev_req.time, curr_req.time 
							back_dup = True
						#else:
						#	print "back connect ", prev_req.time , curr_req.time 
						prev_req.front_seq_req = curr_req
						curr_req.back_seq_req = prev_req

			

				# if j != 1:
				# 	print "j=",j
			
			if front_seq and back_seq:
				break
			j += 1

		if front_seq == True:
			merged_cnt -= 1
			if curr_req.is_read == True:
				merged_read_cnt -= 1
			else:
				merged_write_cnt -= 1

		if back_seq == True:
			if back_dup == False or front_dup == False:
				merged_cnt -= 1
				if curr_req.is_read == True:
					merged_read_cnt -= 1
				else:
					merged_write_cnt -= 1
			# else:
			# 	print "duplicate : ", curr_req.time

		i += 1

	total_size = float(total_size) * 512 / 1024
	read_size = float(read_size) * 512 / 1024
	write_size = float(write_size) * 512 / 1024

	avg_size = float(total_size) / merged_cnt
	if merged_read_cnt != 0:
		avg_read_size = float(read_size) / merged_read_cnt
	else:
		avg_read_size = 0
	if merged_write_cnt != 0:
		avg_write_size = float(write_size) / merged_write_cnt
	else:
		avg_write_size = 0

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


def sort_trace(workloads, **kwargs):
	if len(workloads) < 2:
		print "len(workloads) < 2"
		return

	interval_time = float(kwargs.get('interval_time', -1.0))
	interval_blkcnt = float(kwargs.get('interval_blkcnt', 0))
	new_workloads = []
	i = 0
	start = 0
	while( i < len(workloads) ):
		tmp = []
		start = workloads[i].time
		req = workloads[i]
		blkcnt = workloads[i].blk_count

		while True:
			tmp.append(workloads[i])
			if i < len(workloads) - 1:
				blkcnt += workloads[i+1].blk_count
			curr_time = workloads[i].time
			i+=1

			if  (i < len(workloads) and (blkcnt <= interval_blkcnt or interval_blkcnt == 0) and ( abs(start - curr_time) < interval_time  or interval_time < 0)) == False :
				break
		if len(tmp) != 1:
			print len(tmp)
		tmp.sort(key = lambda x: x.blk_end)
		tmp.sort(key = lambda x: x.blk_start)
		for req in tmp:
			new_workloads.append(req)
	return new_workloads

def analysis_trace2(workloads, **kwargs):
	print "analysis"
	if len(workloads) < 2:
		print "len(workloads) < 2"

	i = 0
	read_cnt = 0
	read_size = 0
	write_cnt = 0
	write_size = 0
	total_cnt = 0
	total_size = 0
	merged_cnt = 0
	merged_read_cnt = 0
	merged_write_cnt = 0

	while(i < len(workloads)):
		curr_req = workloads[i]
		if i==0:
			i+=1
			continue
		prev_req = workloads[i-1]
		if i > 0:
			is_seq = curr_req.is_seq(prev_req, **kwargs)
		else:
			is_seq = False


		total_cnt += 1
		total_size += curr_req.blk_count


		if curr_req.is_read == True:
			read_cnt += 1
			read_size += curr_req.blk_count
		else:
			write_cnt += 1
			write_size += curr_req.blk_count
		
		if is_seq == False:
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
	if merged_read_cnt != 0:
		avg_read_size = float(read_size) / merged_read_cnt
	else:
		avg_read_size = 0
	if merged_write_cnt != 0:
		avg_write_size = float(write_size) / merged_write_cnt
	else:
		avg_write_size = 0

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
if len(sys.argv) < 3 :
	print " Invalid args !!! "  
	print 
	print " python seq_analyzer.py trace_file trace_format [option_name = value]"  
	print
	print " Supported Format"  
	print "   blktrace"  
	print "   disksim"  
	print
	print " Option."  
	print "   Option_Name      Value_type   Default"  
	print "   ignore_dev       Boolean      False"  
	print "   ignore_rw        Boolean      False"  
	print "   overlap          Boolean      True"  
	print "   interval_time    Float        -1.0"  
	print "   interval_blkcnt  Integer      0"  
	print "   load_read        Boolean      True"  
	print "   load_write       Boolean      True"  
	print
	print " Example."  
	print "   python seq_analyzer.py sample.txt blktrace interval_time = 0.01, ignore_rw = False"  
	exit(1)

kwargs = dict(x.split('=', 1) for x in sys.argv[3:])

file_name = sys.argv[1] 
file_format = sys.argv[2] 
workloads = load_trace(file_name, file_format, **kwargs)
workloads = sort_trace(workloads, **kwargs)
analysis_trace2(workloads,**kwargs)
print "load_trace complete !!"
print "Analysis_trace"

#analysis_trace(workloads, **kwargs)
#print_workloads(workloads)
