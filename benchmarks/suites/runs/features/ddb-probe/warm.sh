#!/bin/bash
# warm.sh LABEL REPS: restart flight pods, run Local on each pod concurrently, print per-rep wall (max over pods) and the store_sales stream time
cd /home/vadim/arrowflight-server/ArrowFlight
export KUBECONFIG=/home/vadim/cloudru-k8s/.kube/kubeconfig-surpin.yaml
K="tools/.local/bin/kubectl -n tpch-bench"
S=/tmp/claude-1000/-home-vadim-arrowflight-server/ee026df8-f7be-4efd-965a-fdf4fe615904/scratchpad
D=$S/ddb
$K rollout restart sts/flight >/dev/null; $K rollout status sts/flight --timeout=900s >/dev/null
for p in flight-0 flight-1 flight-2; do $K cp $D/Local.class $p:/tmp/Local.class; done
for p in flight-0 flight-1 flight-2; do ( $K exec $p -- sh -c "cd /tmp && java -Xmx1g -XX:MaxDirectMemorySize=2g --add-opens=java.base/java.nio=ALL-UNNAMED -cp '/opt/arrowflight/arrow-flight-server.jar:/opt/arrowflight/hadoop/*:/tmp' Local grpc+tcp://$p.flight:32010 $p.flight $2" 2>&1 | grep -a " rep " > $S/warm-$1-$p.txt ) & done; wait
python3 - $1 <<'P'
import re,sys,glob
S="/tmp/claude-1000/-home-vadim-arrowflight-server/ee026df8-f7be-4efd-965a-fdf4fe615904/scratchpad/"
res={}
for f in glob.glob(S+"warm-%s-flight-*.txt"%sys.argv[1]):
    for l in open(f):
        m=re.search(r"rep (\d+) wall ([\d.]+)s",l); ss=re.findall(r"store ([\d.]+)s",l)
        res.setdefault(int(m.group(1)),[]).append((float(m.group(2)),max(float(x) for x in ss[:3])))
print(sys.argv[1],"wall per rep (max over pods):"," ".join("%.1f"%max(w for w,_ in res[k]) for k in sorted(res)))
P
