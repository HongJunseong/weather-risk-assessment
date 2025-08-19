# utils/time_kst.py
from datetime import datetime, timedelta

KST = timedelta(hours=9)

def now_kst():
    return datetime.utcnow() + KST

def latest_base_for_ultra():
    """
    초단기 실황/예보 base_date/time 산출.
    우선은 '정시 HH00' 기준으로 사용.
    """
    t = now_kst().replace(minute=0, second=0, microsecond=0)
    return t.strftime("%Y%m%d"), t.strftime("%H") + "00"