#!/usr/bin/env python3
import argparse,json,sys
def fail(m):print("M12 invariant failure: "+m,file=sys.stderr);raise SystemExit(1)
def load(p):return json.load(open(p,encoding="utf-8"))
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--scenario",required=True);ap.add_argument("--result",required=True);a=ap.parse_args();s=load(a.scenario);r=load(a.result)
 if r.get("scenario")!=s.get("name"):fail("scenario mismatch")
 pe=r.get("process_evidence",{});cpids=list(pe.get("controllers",{}).values());ppids=list(pe.get("participants",{}).values())
 if any(not isinstance(x,int) or x<=0 for x in cpids+ppids):fail("invalid PID evidence")
 if len(cpids)!=len(set(cpids)) or len(ppids)!=len(set(ppids)):fail("processes are not distinct")
 settled=set(s.get("expectations",{}).get("settled_checkpoints",[]));reps={x["name"]:x["replicas"] for x in s["cluster"].get("resources",[])};cps=r.get("checkpoints",[])
 if not cps:fail("no checkpoints")
 for c in cps:
  n=c["name"];active=c.get("controllers",{}).get("active",[])
  if len(active)!=1:fail(f"{n}: expected exactly one active controller, got {active}")
  live=c.get("live_instances",{});acs=c.get("active_current_state",{})
  for i,b in acs.items():
   if i not in live:fail(f"{n}: active CurrentState for dead {i}")
   if b.get("session")!=live[i]:fail(f"{n}: stale CurrentState session for {i}")
  for t in c.get("pending_transitions",[]):
   if live.get(t.get("instance"))!=t.get("target_session"):fail(f"{n}: stale transition target {t}")
  if n in settled and c.get("pending_transitions"):fail(f"{n}: settled but transitions remain")
  if n in settled:
   derived={}
   for instance,bundle in acs.items():
    for res,parts in bundle.get("resources",{}).items():
     for part,state in parts.items():derived.setdefault(res,{}).setdefault(part,{})[instance]=state
   if derived!=c.get("external_view",{}):fail(f"{n}: ExternalView does not match active CurrentState")
  for res,parts in c.get("external_view",{}).items():
   for part,states in parts.items():
    leaders=[i for i,v in states.items() if v=="LEADER" and i in live]
    if len(leaders)>1:fail(f"{n}: {res}/{part} multiple leaders {leaders}")
    ar=[i for i,v in states.items() if v in ("LEADER","STANDBY") and i in live]
    if n in settled and ar and len(leaders)!=1:fail(f"{n}: {res}/{part} lacks one leader")
    rf=reps.get(res)
    if n in settled and rf is not None and len(live)>=rf and len(ar)!=rf:fail(f"{n}: {res}/{part} RF {len(ar)} != {rf}")
  for rr in c.get("routing_results",[]):
   res=rr.get("resource");part=rr.get("partition");state=rr.get("state");expected=set(rr.get("instances",[]));actual={i for i,v in c.get("external_view",{}).get(res,{}).get(part,{}).items() if v==state and i in live}
   if expected!=actual:fail(f"{n}: routing mismatch for {res}/{part}/{state}: {expected} != {actual}")
 exp=s.get("expectations",{});ev=r.get("controller_events",[]);fs=[x for x in ev if x.get("kind") in ("leader_crash","lease_loss","controller_failover")]
 if len(fs)<exp.get("min_controller_failovers",0):fail("too few controller failovers")
 if exp.get("require_stale_controller_fence") and not any(x.get("old_controller_fenced") and x.get("stale_publish_rejected") for x in ev):fail("no stale-controller fence evidence")
 if exp.get("require_session_replacement"):
  seen={};chg=False
  for c in cps:
   for i,se in c.get("live_instances",{}).items():
    if i in seen and seen[i]!=se:chg=True
    seen[i]=se
  if not chg:fail("participant session replacement not observed")
if __name__=="__main__":main()
