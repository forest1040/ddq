import json,sys,statistics as st,collections
for f in sys.argv[1:]:
    a=json.load(open(f))['adaptive']
    rows={r['variant_id']:r for r in a['rows']}
    out={}
    for v,r in rows.items():
        ts=r['trials']
        out[v]=dict(status=r['status'],n=len(ts),med_wall=st.median(t['wall_seconds'] for t in ts if t['wall_seconds']) if ts else None,
          med_exec=st.median(t['metrics'].get('execute_seconds',float('nan')) for t in ts))
    c=a['comparisons'][0] if a['comparisons'] else {}
    pm=c.get('paired_metrics',{}).get('execute_seconds',{})
    autos=rows['auto']['trials']
    sw=[t['metrics'].get('switch_count') for t in autos]; ver=[t['metrics'].get('verified') for t in autos]
    pg=[round(t['metrics']['predicted_gain_lower_seconds'],3) for t in autos if 'predicted_gain_lower_seconds' in t['metrics']]
    reasons=collections.Counter(); upd=collections.Counter()
    for t in autos:
        ad=t['raw']['adaptive']
        for d in ad['replan_decisions']:
            reasons[(d['kind'],d.get('reason'),d.get('selected') and str(d.get('selected')))]+=1
            if d['kind']=='apply': upd[str(d.get('updates'))]+=1
    print(f,json.dumps(out),'\n switch',sw,'verified',ver,'\n exec diff med',pm.get('difference',{}).get('median'),'ci',pm.get('difference_ci95'),'wall ci',c.get('difference_ci95'),
      '\n pred_gain',pg,'positive',a.get('pilot_positive_gain_verified'),'\n applied',dict(upd),'\n',dict(reasons))
    # candidate reasons
    cr=collections.Counter()
    for t in autos:
        for d in t['raw']['adaptive']['replan_decisions']:
            for cd in d.get('candidates',[]) or []: cr[(str(cd.get('updates')),cd.get('reason'),cd.get('eligible'))]+=1
    print(' cand',dict(cr))
