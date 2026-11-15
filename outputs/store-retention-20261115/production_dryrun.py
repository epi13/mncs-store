import json,time,os,resource,struct,sys
from pathlib import Path
from mncs_store import EmbeddedStore
root=Path(sys.argv[2]).resolve(); output=Path(sys.argv[1]).resolve(); assert not output.is_relative_to(root); output.mkdir(parents=True,exist_ok=True)
start=time.monotonic();before=(root/'head').read_bytes();heads={'MG':{'files':0,'bytes':0},'MD':{'files':0,'bytes':0},'other':{'files':0,'bytes':0}};complete=True
with os.scandir(root/'generations') as entries:
 for n,entry in enumerate(entries):
  if n>=150000 or time.monotonic()-start>15:complete=False;break
  if not entry.is_file(follow_symlinks=False):heads['other']['files']+=1;continue
  with open(entry.path,'rb') as stream:magic=stream.read(2).decode('ascii',errors='replace')
  group=heads.get(magic,heads['other']);group['files']+=1;group['bytes']+=entry.stat(follow_symlinks=False).st_size
print(json.dumps({'header_inventory_seconds':time.monotonic()-start,'complete':complete,'encodings':heads}),flush=True)
with EmbeddedStore(root, read_only=True) as store:
 result=store.prune_generations(keep_last=4096,dry_run=True,max_files=120000,max_bytes=512*1024*1024,max_seconds=20)
 result.pop('candidates',None);result.pop('removed',None)
 result.update(authorization='hypothetical window only; no Environment retention policy approved',production_deleted=False)
print(json.dumps(result),flush=True)
summary={'header_inventory':heads,'header_complete':complete,'dry_run':result,'head_before':struct.unpack('>Q',before[4:])[0],'head_after':struct.unpack('>Q',(root/'head').read_bytes()[4:])[0],'wall_seconds':time.monotonic()-start,'max_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
(output/'production-dryrun.json').write_text(json.dumps(summary,indent=2)+'\n')
