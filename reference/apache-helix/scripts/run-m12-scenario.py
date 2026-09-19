#!/usr/bin/env python3
import json,os,pathlib,signal,subprocess,sys,time
TIMEOUT=float(os.environ.get("CLUSTODIAN_M12_INNER_TIMEOUT_SECONDS","120"));POLL=.05
class H:
 def __init__(self,s):
  self.s=s;self.cluster=s["name"];self.work=pathlib.Path(req("CLUSTODIAN_M12_WORK_DIR"));self.work.mkdir(parents=True,exist_ok=True);self.bin=pathlib.Path(req("CLUSTODIAN_M12_HARNESS_DIR"));self.c={};self.p={};self.dead=[];self.raw2log={};self.cp=[];self.ev=[];self.mem={};self.active_before=None;self.bar=self.work/"barrier";self.bar.mkdir(exist_ok=True);self.env=os.environ.copy();self.env.update({"CLUSTODIAN_M12_CLUSTER":self.cluster})
 def cmd(self,n,*a,env=None):
  e=self.env.copy();e.update(env or {});x=subprocess.run([str(self.bin/n),*map(str,a)],env=e,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
  if x.returncode:raise RuntimeError(f"{n} failed: {x.stderr}")
  return x.stdout.strip()
 def snap(self):return json.loads(self.cmd("m12-probe","snapshot"))
 def wait(self,f,w):
  end=time.monotonic()+TIMEOUT;last=None
  while time.monotonic()<end:
   try:
    if f():return
   except Exception as e:last=e
   time.sleep(POLL)
  raise TimeoutError(f"timeout waiting for {w}; {last}; {self.snap()}")
 def startc(self,i):
  if i in self.c and self.c[i].poll() is None:return
  e=self.env.copy();e["CLUSTODIAN_M12_CONTROLLER_ID"]=i;l=open(self.work/f"c-{i}.log","ab",buffering=0);self.c[i]=subprocess.Popen([str(self.bin/"m12-controller")],env=e,stdout=l,stderr=l)
 def startp(self,sp,label):
  i=sp["id"];e=self.env.copy();e.update({"CLUSTODIAN_M12_INSTANCE_ID":i,"CLUSTODIAN_M12_ZONE":sp["zone"],"CLUSTODIAN_M12_BARRIER_DIR":str(self.bar)});l=open(self.work/f"p-{i}.log","ab",buffering=0);self.p[i]=subprocess.Popen([str(self.bin/"m12-participant")],env=e,stdout=l,stderr=l);self.wait(lambda:i in self.snap().get("live_instances",{}),i+" live");raw=self.snap()["live_instances"][i];self.raw2log[raw]=label
 def stop(self,p,sig=signal.SIGTERM):
  if p and p.poll() is None:
   p.send_signal(sig)
   try:p.wait(5)
   except subprocess.TimeoutExpired:p.kill();p.wait(5)
 def active(self):
  a=self.snap().get("controllers",{}).get("active",[]);return a[0] if len(a)==1 else None
 def controller_visible(self,name):
  c=self.snap().get("controllers",{});return name in c.get("active",[]) or name in c.get("standby",[])
 def one(self,diff=None):self.wait(lambda:self.active() is not None and self.active()!=diff,"single active")
 def canon(self,s):
  cv=lambda x:self.raw2log.get(x,x);live={i:cv(v) for i,v in s.get("live_instances",{}).items()};acs={i:{"session":cv(b.get("session")),"resources":b.get("resources",{})} for i,b in s.get("active_current_state",{}).items()};pend=[]
  for t in s.get("pending_transitions",[]):x=dict(t);x["target_session"]=cv(x.get("target_session"));pend.append(x)
  ctr=s.get("controllers",{});return {"controllers":{"active":list(ctr.get("active",[])),"standby":list(ctr.get("standby",[]))},"live_instances":live,"active_current_state":acs,"external_view":s.get("external_view",{}),"pending_transitions":pend,"routing_results":s.get("routing_results",[])}
 def checkpoint(self,n):self.cp.append({"name":n,**self.canon(self.snap())})
 def converged(self):
  x=self.canon(self.snap())
  if len(x["controllers"]["active"])!=1 or x["pending_transitions"]:return False
  live=x["live_instances"];ev=x["external_view"];derived={}
  for instance,bundle in x["active_current_state"].items():
   for resource,partitions in bundle.get("resources",{}).items():
    for partition,state in partitions.items():derived.setdefault(resource,{}).setdefault(partition,{})[instance]=state
  if derived!=ev:return False
  for r in self.s["cluster"].get("resources",[]):
   parts=ev.get(r["name"],{})
   if len(parts)!=r["partitions"]:return False
   for states in parts.values():
    ar=[i for i,v in states.items() if i in live and v in ("LEADER","STANDBY")]
    leaders=[i for i,v in states.items() if i in live and v=="LEADER"]
    if ar and len(leaders)!=1:return False
    if len(live)>=r["replicas"] and len(ar)!=r["replicas"]:return False
  return True
 def config(self):
  self.cmd("m12-probe","ensure-cluster")
  for p in self.s["cluster"].get("participants",[]):self.cmd("m12-probe","put-instance",p["id"],p["zone"])
 def create_resources(self):
  d=self.work/"r";d.mkdir(exist_ok=True)
  for r in self.s["cluster"].get("resources",[]):
   q=d/(r["name"]+".json");q.write_text(json.dumps(r));self.cmd("m12-probe","put-resource-json",q)
 def step(self,t):
  o=t["op"]
  if o=="start_controllers":[self.startc(x) for x in t["controllers"]]
  elif o=="await_single_active_controller":self.one(self.active_before if t.get("require_different_term") else None)
  elif o=="start_participants":
   m={x["id"]:x for x in self.s["cluster"]["participants"]}
   for i in t["instances"]:self.startp(m[i],m[i].get("session",i+"-1"))
  elif o=="start_participant":sp=next(x for x in self.s["cluster"]["participants"] if x["id"]==t["instance"]);self.startp(sp,sp.get("session",sp["id"]+"-1"))
  elif o=="stop_participant":i=t["instance"];self.stop(self.p.get(i));self.wait(lambda:i not in self.snap().get("live_instances",{}),i+" offline")
  elif o=="restart_participant":
   i=t["instance"];oldraw=self.snap().get("live_instances",{}).get(i);self.stop(self.p.get(i));self.wait(lambda:i not in self.snap().get("live_instances",{}),i+" offline");sp=next(x for x in self.s["cluster"]["participants"] if x["id"]==i);self.startp(sp,t["new_session_label"]);self.mem["old-session:"+i]=oldraw
  elif o=="create_resources":self.create_resources()
  elif o=="await_converged":self.wait(self.converged,"convergence")
  elif o=="checkpoint":self.checkpoint(t["name"])
  elif o=="kill_active_controller":i=self.active();assert i;self.active_before=i;self.stop(self.c[i],signal.SIGKILL);self.dead.append(i);self.ev.append({"kind":"leader_crash","new_controller_elected":True})
  elif o=="pause_active_controller_until_lease_expires":i=self.active();assert i;self.active_before=i;os.kill(self.c[i].pid,signal.SIGSTOP);self.one(i);self.ev.append({"kind":"lease_loss","new_controller_elected":True,"old_controller_fenced":True})
  elif o=="resume_stale_controller":
   if self.active_before and self.c[self.active_before].poll() is None:os.kill(self.c[self.active_before].pid,signal.SIGCONT);time.sleep(.2)
  elif o=="restart_last_killed_controller":self.startc(self.dead[-1])
  elif o=="restart_one_standby_controller":i=self.snap()["controllers"]["standby"][0];self.stop(self.c[i]);self.startc(i)
  elif o=="assert_active_controller_unchanged":
   cur=self.active()
   if self.active_before is None:self.active_before=cur
   elif cur!=self.active_before:raise AssertionError("active controller changed")
  elif o=="assert_restarted_controller_did_not_preempt":
   if self.active()==self.dead[-1]:raise AssertionError("old leader preempted")
   self.ev.append({"kind":"old_controller_rejoin","restarted_controller_did_not_preempt":True})
  elif o=="block_next_application_transition":(self.bar/"block-next").write_text("1")
  elif o=="await_pending_transition":self.wait(lambda:bool(self.snap().get("pending_transitions")),"pending transition");self.wait(lambda:(self.bar/"blocked").exists(),"blocked callback")
  elif o=="release_blocked_application_transition":(self.bar/"release").write_text("1")
  elif o=="kill_partition_leader":
   st=self.snap()["external_view"][t["resource"]][t["partition"]];ls=[i for i,v in st.items() if v=="LEADER"];assert len(ls)==1;i=ls[0];self.mem[t.get("remember_as","participant")]=i;self.stop(self.p[i],signal.SIGKILL);self.wait(lambda:i not in self.snap().get("live_instances",{}),i+" offline")
  elif o=="restart_remembered_participant":i=self.mem[t["name"]];sp=next(x for x in self.s["cluster"]["participants"] if x["id"]==i);self.startp(sp,t["new_session_label"])
  elif o=="kill_active_controller_and_partition_leader":self.step({"op":"kill_active_controller"});self.step({"op":"kill_partition_leader","resource":t["resource"],"partition":t["partition"]})
  elif o=="assert_old_session_state_persisted":
   i=t["instance"];raw=self.mem.get("old-session:"+i);assert raw;d=json.loads(self.cmd("m12-probe","raw-session-state-present",i,raw));
   if not d.get("present"):raise AssertionError("old session CurrentState was not retained")
  elif o=="run_direct_fence_probe":self.fence()
  else:raise RuntimeError("unknown op "+o)
 def fence(self):
  d=self.work/"fence";d.mkdir(exist_ok=True);e=self.env.copy();e.update({"CLUSTODIAN_M12_CONTROLLER_ID":"fence-old","CLUSTODIAN_M12_FENCE_DIR":str(d)});p=subprocess.Popen([str(self.bin/"m12-fence-probe")],env=e,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
  self.wait(lambda:(d/"acquired").exists(),"fence acquired");os.kill(p.pid,signal.SIGSTOP)
  self.wait(lambda:not self.controller_visible("fence-old"),"fence-old lease expiration")
  for cid in self.s["cluster"]["controllers"]:self.startc(cid)
  self.one("fence-old");os.kill(p.pid,signal.SIGCONT);(d/"attempt").write_text("1");out,err=p.communicate(timeout=10)
  if p.returncode:raise RuntimeError(f"m12-fence-probe failed: {err.strip()}")
  data=json.loads(out);assert data["accepted"] is False;self.ev.append({"kind":"lease_loss","old_controller_fenced":True,"stale_publish_rejected":True,"new_controller_elected":True})
 def run(self):
  self.config()
  for t in self.s["steps"]:self.step(t)
  return {"result_schema_version":1,"scenario":self.s["name"],"process_evidence":{"controllers":{k:v.pid for k,v in self.c.items()},"participants":{k:v.pid for k,v in self.p.items()}},"checkpoints":self.cp,"controller_events":self.ev}
 def cleanup(self):
  for p in list(self.p.values())+list(self.c.values()):self.stop(p)
def req(k):
 v=os.environ.get(k)
 if not v:raise RuntimeError("missing "+k)
 return v
def main():
 s=json.load(open(sys.argv[1],encoding="utf-8"));h=H(s)
 try:print(json.dumps(h.run(),sort_keys=True))
 finally:h.cleanup()
if __name__=="__main__":main()
