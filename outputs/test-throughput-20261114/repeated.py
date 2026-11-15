import sys,time,json,resource
from pathlib import Path
sys.path[:0]=['tests','python']
import mncs_exec as transport
import retained_session as retained
from mncs_exec import call_many,BYTES
from retained_session import METRICS,close_reference_sessions
rows=[]
compiler_processes=0
original=transport._run
def counted(*args,**kwargs):
 global compiler_processes
 compiler_processes+=1
 return original(*args,**kwargs)
transport._run=retained._run=counted

for iteration in range(3):
 start=time.monotonic()
 result=call_many('src/store/manifest.mncs','store.manifest.v1',[('probe','v2_total_len',[BYTES(bytes(range(28)))])],'mncs-research-bytecode')
 rows.append({'elapsed':time.monotonic()-start,'status':result['probe']['status'],'returned':result['probe']['returned'],'steps':result['probe']['steps'],'result_json_bytes':len(json.dumps(result,separators=(',',':')).encode())})
close_reference_sessions()
print(json.dumps({'rows':rows,'metrics':METRICS,'compiler_processes':compiler_processes,'user_cpu':resource.getrusage(resource.RUSAGE_SELF).ru_utime+resource.getrusage(resource.RUSAGE_CHILDREN).ru_utime,'system_cpu':resource.getrusage(resource.RUSAGE_SELF).ru_stime+resource.getrusage(resource.RUSAGE_CHILDREN).ru_stime}))
