import json,os,signal,subprocess,time
from pathlib import Path
ROOT=Path('/home/epi13/Documents/Projects')
OUT=Path('/tmp/mncs-throughput-20261114')
def value(n,bits=32):return {'integer':{'value':n,'type':{'bits':bits,'signed':False}}}
def seq(n):return {'sequence':{'values':[{'byte':{'value':i%256}} for i in range(n)]}}
rows=[]
for profile in ('debug','release'):
 for source,module,fn,args in [('identity','store.identity.v1','object_encode',[value(1),value(1,64)]),('manifest','store.manifest.v1','v2_total_len',[seq(28)])]:
  corpus=OUT/f'{profile}-{source}.json';corpus.write_text(json.dumps({'schema_version':'0.1','name':'bounded-diagnostic','cases':[{'id':'probe','request':{'schema_version':'0.1','target':{'module':module,'function':fn},'arguments':args,'step_budget':32768}}]}))
  argv=[str(ROOT/f'mncs-language/target/{profile}/mncs'),'experiment','run',str(ROOT/f'mncs-store/src/store/{source}.mncs'),'--backend','mncs-research-bytecode','--corpus',str(corpus)]
  env=dict(os.environ,MNCS_LIBRARY_PATH=f'{ROOT}/mncs-stdlib/library:{ROOT}/mncs-store/src')
  output=OUT/f'{profile}-{source}.out';err=OUT/f'{profile}-{source}.err';metrics=OUT/f'{profile}-{source}.time'
  start=time.perf_counter()
  with output.open('w') as stdout,err.open('w') as stderr:
   p=subprocess.Popen(['/usr/bin/time','-v','-o',str(metrics),*argv],stdout=stdout,stderr=stderr,env=env,start_new_session=True)
   timed_out=False
   try:p.wait(timeout=45)
   except subprocess.TimeoutExpired:
    timed_out=True;os.killpg(p.pid,signal.SIGTERM)
    try:p.wait(timeout=2)
    except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
  row={'profile':profile,'source':source,'elapsed':time.perf_counter()-start,'code':p.returncode,'incomplete':timed_out,'output_bytes':output.stat().st_size}
  rows.append(row);print(json.dumps(row),flush=True)
  (OUT/'samples.json').write_text(json.dumps(rows,indent=2))
