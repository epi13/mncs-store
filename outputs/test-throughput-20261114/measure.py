import sys,os,time,json,resource,tempfile
from pathlib import Path
ROOT=Path.cwd();sys.path.insert(0,str(ROOT/'tests'));sys.path.insert(0,str(ROOT/'python'))
import mncs_exec as transport
import retained_session as retained
from store_phase1a import Engine
from store_phase2 import StorePhase2
from test_phase2 import test_general_blob_boundaries_roundtrip
name=sys.argv[1];destination=Path(sys.argv[2]);started=time.monotonic();status='UNKNOWN';error=None
counts={'compiler_processes':0,'batches':0,'cases':0};original=transport._run

def counted(*args,**kwargs):
 counts['compiler_processes']+=1
 return original(*args,**kwargs)
transport._run=retained._run=counted
engine=Engine();original_run=engine.run

def run(source,module,calls,grants=()):
 counts['batches']+=1;counts['cases']+=len(calls)
 if counts['batches']%100==0: print(json.dumps({'elapsed':time.monotonic()-started,**counts}),flush=True)
 return original_run(source,module,calls,grants)
engine.run=run
try:
 with tempfile.TemporaryDirectory(prefix='mncs-reference-measure-') as root:
  path=str(Path(root)/'store')
  if name=='blob':test_general_blob_boundaries_roundtrip(path,engine)
  else:
   size=int(name);data=bytes(i%256 for i in range(size));s=StorePhase2.create(path,engine);oid=s.put_blob(data)
   assert s.get_blob(oid)==data;s.close();s=StorePhase2.open(path,engine)
   assert s.verify(oid);assert s.get_blob(oid)==data;s.close()
 status='PASS'
except BaseException as exc:
 error=repr(exc)
 raise
finally:
 retained.close_reference_sessions()
 own=resource.getrusage(resource.RUSAGE_SELF);children=resource.getrusage(resource.RUSAGE_CHILDREN)
 result={'workload':name,'status':status,'error':error,'elapsed':time.monotonic()-started,**counts,
 'reference':dict(retained.METRICS),'user_cpu':own.ru_utime+children.ru_utime,'system_cpu':own.ru_stime+children.ru_stime,
 'self_peak_rss_kib':own.ru_maxrss,'child_peak_rss_kib':children.ru_maxrss}
 destination.write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)
