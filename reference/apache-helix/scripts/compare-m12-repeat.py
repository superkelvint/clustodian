#!/usr/bin/env python3
import argparse,json,sys

def load(p):return json.load(open(p,encoding="utf-8"))
def norm_pending(xs):return sorted(xs,key=lambda x:(x.get("resource",""),x.get("partition",""),x.get("instance",""),x.get("target_session",""),x.get("from",""),x.get("to","")))
def norm_routing(xs):return sorted(xs,key=lambda x:(x.get("resource",""),x.get("partition",""),x.get("state",""),tuple(sorted(x.get("instances",[])))))
def canon(result,scenario):
 settled=set(scenario.get("expectations",{}).get("settled_checkpoints",[]));out={"scenario":result.get("scenario"),"checkpoints":[]}
 for c in result.get("checkpoints",[]):
  ctr=c.get("controllers",{});base={"name":c.get("name"),"active_count":len(ctr.get("active",[])),"standby_count":len(ctr.get("standby",[])),"live_instances":c.get("live_instances",{})}
  if c.get("name") in settled:
   base.update({"active_current_state":c.get("active_current_state",{}),"external_view":c.get("external_view",{}),"pending_transitions":norm_pending(c.get("pending_transitions",[])),"routing_results":norm_routing(c.get("routing_results",[]))})
  else:
   base["has_pending_transitions"]=bool(c.get("pending_transitions",[]))
  out["checkpoints"].append(base)
 kinds={}
 for e in result.get("controller_events",[]):
  k=(e.get("kind"),bool(e.get("old_controller_fenced",False)),bool(e.get("stale_publish_rejected",False)),bool(e.get("new_controller_elected",False)),bool(e.get("restarted_controller_did_not_preempt",False)))
  kinds[k]=kinds.get(k,0)+1
 out["events"]=sorted((list(k)+[v]) for k,v in kinds.items())
 return out
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--scenario",required=True);ap.add_argument("--left",required=True);ap.add_argument("--right",required=True);a=ap.parse_args();s=load(a.scenario);l,r=canon(load(a.left),s),canon(load(a.right),s)
 if l!=r:
  print("M12 repeatability mismatch",file=sys.stderr);print(json.dumps(l,indent=2,sort_keys=True),file=sys.stderr);print(json.dumps(r,indent=2,sort_keys=True),file=sys.stderr);raise SystemExit(1)
if __name__=="__main__":main()
