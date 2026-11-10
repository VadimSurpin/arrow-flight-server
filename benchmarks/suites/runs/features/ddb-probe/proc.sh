#!/bin/bash
# proc.sh LABEL REPS: like warm.sh (current sts settings), plus a per-second sample of the server process (utime stime minflt rss) in each pod
cd /home/vadim/arrowflight-server/ArrowFlight
export KUBECONFIG=/home/vadim/cloudru-k8s/.kube/kubeconfig-surpin.yaml
K="tools/.local/bin/kubectl -n tpch-bench"
S=/tmp/claude-1000/-home-vadim-arrowflight-server/ee026df8-f7be-4efd-965a-fdf4fe615904/scratchpad
$K rollout restart sts/flight >/dev/null; $K rollout status sts/flight --timeout=900s >/dev/null
for p in flight-0 flight-1 flight-2; do $K cp $S/ddb/Local.class $p:/tmp/Local.class
  $K exec $p -- sh -c ': > /tmp/proc.log; PID=$(ps -eo pid,args | awk "/HadoopArrowFlightServer/ && !/awk/{print \$1; exit}"); (for i in $(seq 1 120); do echo $(date +%s%3N) $(awk "{print \$14,\$15,\$10}" /proc/$PID/stat) $(awk "/VmRSS/{print \$2}" /proc/$PID/status) >> /tmp/proc.log; sleep 1; done) >/dev/null 2>&1 &'; done
for p in flight-0 flight-1 flight-2; do ( $K exec $p -- sh -c "cd /tmp && java -Xmx1g -XX:MaxDirectMemorySize=2g --add-opens=java.base/java.nio=ALL-UNNAMED -cp '/opt/arrowflight/arrow-flight-server.jar:/opt/arrowflight/hadoop/*:/tmp' Local grpc+tcp://$p.flight:32010 $p.flight $2" 2>&1 | grep -a " rep " > $S/proc-$1-$p.txt ) & done; wait
for p in flight-0 flight-1 flight-2; do $K exec $p -- cat /tmp/proc.log > $S/procs-$1-$p.log; done
python3 - $1 <<'P'
import re,sys
S="/tmp/claude-1000/-home-vadim-arrowflight-server/ee026df8-f7be-4efd-965a-fdf4fe615904/scratchpad/"
lab=sys.argv[1]
for p in ("flight-0","flight-1","flight-2"):
    samples=[tuple(map(int,l.split())) for l in open(S+"procs-%s-%s.log"%(lab,p)) if len(l.split())==5]
    print(p)
    for l in open(S+"proc-%s-%s.txt"%(lab,p)):
        m=re.search(r"rep (\d+) wall ([\d.]+)s.*ms=(\d+)\.\.(\d+)",l)
        rep,wall,a,b=int(m.group(1)),float(m.group(2)),int(m.group(3)),int(m.group(4))
        s0=min(samples,key=lambda x:abs(x[0]-a)); s1=min(samples,key=lambda x:abs(x[0]-b))
        print("  rep %d wall %.1fs  user %.1fs sys %.1fs minflt %.0fk rss %d MB"%(rep,wall,(s1[1]-s0[1])/100,(s1[2]-s0[2])/100,(s1[3]-s0[3])/1000,s1[4]/1024))
P
