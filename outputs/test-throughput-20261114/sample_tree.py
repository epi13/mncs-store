import os,sys,subprocess,time,json,signal
from pathlib import Path
out=Path('/tmp/mncs-throughput-20261114')
for mode in ['cli','retained']:
 env=dict(os.environ)
 if mode=='cli':env['MNCS_TEST_TRANSPORT']='cli'
 else:env.pop('MNCS_TEST_TRANSPORT',None)
 start=time.monotonic();p=subprocess.Popen([sys.executable,str(out/'repeated.py')],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
 peak=0;max_processes=0
 try:
  while p.poll() is None:
   rows={}
   for entry in Path('/proc').iterdir():
    if not entry.name.isdecimal():continue
    try:
     content=(entry/'stat').read_text();fields=content[content.rfind(')')+2:].split();rows[int(entry.name)]=(int(fields[1]),int(fields[21])*os.sysconf('SC_PAGE_SIZE')//1024)
    except (OSError,ValueError,IndexError):pass
   selected={p.pid}
   for _ in range(10):
    nextset=selected|{pid for pid,(parent,rss) in rows.items() if parent in selected}
    if nextset==selected:break
    selected=nextset
   peak=max(peak,sum(rows.get(pid,(0,0))[1] for pid in selected));max_processes=max(max_processes,len(selected))
   if time.monotonic()-start>30:
    p.send_signal(signal.SIGINT);p.wait(timeout=8);break
   time.sleep(.02)
  stdout,stderr=p.communicate(timeout=8)
  record={'mode':mode,'code':p.returncode,'elapsed':time.monotonic()-start,'sampled_tree_peak_rss_kib':peak,'max_processes':max_processes,'sampling_interval_s':.02,'result':json.loads(stdout) if stdout else None,'stderr':stderr.decode()[-1000:]}
  (out/f'repeated-{mode}.json').write_text(json.dumps(record,indent=2));print(json.dumps(record),flush=True)
 finally:
  if p.poll() is None:p.kill();p.communicate()
