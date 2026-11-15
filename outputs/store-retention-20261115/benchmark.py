"""Isolated platform-cost benchmark; semantic double, native proofs separate."""
import sys,time,json,resource,importlib.util,os
from pathlib import Path
from mncs_store import EmbeddedStore
from mncs_store.embedded import CrashInjected
from test_embedded_store import _session
mode,count,out=sys.argv[1],int(sys.argv[2]),Path(sys.argv[3]);root=out.parent/out.stem
layout=sys.argv[4] if len(sys.argv)>4 else 'delta'
from mncs_store import embedded as implementation
original_interval=implementation.GENERATION_CHECKPOINT_INTERVAL
if layout=='legacy':implementation.GENERATION_CHECKPOINT_INTERVAL=1
assert root.resolve().is_relative_to(Path('/tmp').resolve()) and not root.exists(), 'use a new isolated directory under /tmp'
if mode=='baseline':
 spec=importlib.util.spec_from_file_location('mncs_store.baseline_embedded',Path(os.environ['MNCS_RETENTION_BASELINE_MODULE']))
 module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module);Store=module.EmbeddedStore
else:Store=EmbeddedStore
with EmbeddedStore(root,session=_session()) as store:
 for n in range(count):
  store.put_bound_object(domain_schema=b'benchmark/1',domain_identity=str(n).encode(),descriptor=b'bytes',payload=(n%32).to_bytes(4,'big')*1024,expected_generation=n)
 implementation.GENERATION_CHECKPOINT_INTERVAL=original_interval
 def crash(name):
  if name=='before_generation_publication':raise CrashInjected(name)
 store._failpoint=crash
 try:store.put_bound_object(domain_schema=b'benchmark/1',domain_identity=b'aborted',descriptor=b'bytes',payload=b''.join(bytes([128+i])*65536 for i in range(32)),expected_generation=count)
 except CrashInjected:pass
 else:raise AssertionError('fault not injected')

def measure(fn):
 u=resource.getrusage(resource.RUSAGE_SELF);t=time.perf_counter();v=fn();w=resource.getrusage(resource.RUSAGE_SELF)
 return v,{'wall':time.perf_counter()-t,'cpu':w.ru_utime+w.ru_stime-u.ru_utime-u.ru_stime}
def occupied():return sum(p.stat().st_size for p in root.rglob('*') if p.is_file())
_,recovery=measure(lambda: Store(root,session=_session(),verify_on_open=False).close())
size_before=occupied()
store,opening=measure(lambda: Store(root,session=_session(),verify_on_open=False))
_,current_cold=measure(lambda:store.get_bound_object(b'benchmark/1',str(count-1).encode()))
_,current_warm=measure(lambda:[store.get_bound_object(b'benchmark/1',str(count-1).encode()) for _ in range(20)])
_,historical=measure(lambda:store.domain_bindings_at(max(0,count-3)))
store.close()
options={'session':_session(),'verify_on_open':False}
if mode!='baseline':options['maintenance']=True
with Store(root,**options) as store:
 floor=store.retention_floor(keep_last=32)
 before=store.objects_at(floor);head=store.objects_at(count)
 # Isolate measured Python filesystem calls from fixture construction and
 # semantic admission. This is not an aggregate kernel syscall count.
 import os
 calls={name:0 for name in ['open','stat','unlink','fsync','replace']};original={name:getattr(os,name) for name in calls}
 def counter(name):
  def call(*args,**kwargs):calls[name]+=1;return original[name](*args,**kwargs)
  return call
 for name in calls:setattr(os,name,counter(name))
 args={'keep_last':32,'dry_run':False}
 if mode!='baseline':args.update(policy_id='isolated-benchmark-window-32',reclaim_chunks=True,audit_path=out.with_suffix('.audit'),max_files=100000,max_bytes=64*1024*1024,max_seconds=60)
 before_allocation={str(p.relative_to(root)):p.stat().st_blocks*512 for p in root.rglob('*') if p.is_file()}
 try:result,maintenance=measure(lambda:store.prune_generations(**args))
 finally:
  for name,fn in original.items():setattr(os,name,fn)
 reclaimed_allocation=sum(blocks for name,blocks in before_allocation.items() if not (root/name).exists())
 assert store.objects_at(floor)==before and store.objects_at(count)==head
 assert store.current_generation==count
 _,transaction=measure(lambda:store.put_bound_object(domain_schema=b'benchmark/1',domain_identity=b'after',descriptor=b'bytes',payload=b'after',expected_generation=count))
size_after=occupied()
with Store(root,session=_session(),read_only=True) as read:
 _,post_current=measure(lambda:read.get_bound_object(b'benchmark/1',str(count-1).encode()))
 _,post_historical=measure(lambda:read.domain_bindings_at(count-3))
result.pop('candidates',None);result.pop('removed',None)
summary={'mode':mode,'generations':count,'layout':layout,'reclaimed_allocated_bytes':reclaimed_allocation,'semantics':'platform-only semantic-session double; native authority tested separately','recovery':recovery,'reopen':opening,'current_cold':current_cold,'current_20_warm':current_warm,'historical':historical,'maintenance':maintenance,'transaction':transaction,'post_current':post_current,'post_historical':post_historical,'physical_bytes_before':size_before,'physical_bytes_after_including_one_new_transaction':size_after,'python_filesystem_calls_during_maintenance':calls,'result':result,'max_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
out.write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary),flush=True)
