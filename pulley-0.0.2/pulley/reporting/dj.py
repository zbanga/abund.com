import os
import sys

sys.path.insert(0, '/mnt/qc_site')
sys.path.insert(1, '/mnt')
os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'

from django.contrib.auth.models import User
from apps.scanner.models import Scan, ScanBacktest
from apps.backtester.models import Algorithm

# reload(Scan)
# reload(ScanBacktest)
# reload(Algorithm)

def dump_results(zp_sim_params, zp_results, name,
                 iL=0,
                 username='jimmie'):

    user = User.objects.get(username=username)
    scan, created = Scan.objects.get_or_create(user=user, name=name, query='')
    algo, created = Algorithm.objects.get_or_create(name=name, user=user)
    bt, created = ScanBacktest.objects.get_or_create(scan=scan, algorithm=algo, name=name)

    ga_rep, zp_risk_report, zp_risk_metrics, yoy = bt.get_bt_results(zp_sim_params=zp_sim_params,
                                                                zp_results=zp_results,
                                                                iL=iL)
    bt.dump_bt_results(zp_sim_params,
                       zp_results,
                       ga_rep=ga_rep,
                       zp_risk_report=zp_risk_report,
                       zp_risk_metrics=zp_risk_metrics)
    scan.save()
    algo.save()
    bt.save()
