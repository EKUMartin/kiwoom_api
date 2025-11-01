import os, json, requests
from datetime import datetime,date, time as dtime
from pydantic import BaseModel
import schedule
import time 
import psycopg2
from psycopg2 import sql
#---------------------?‘ê·¼? í° ë°œê¸‰---------------------------#
def fn_au10001(data):
    # 1. ?”ì²­??API URL
    host = 'https://mockapi.kiwoom.com' # ëª¨ì˜?¬ì
    # host = 'https://api.kiwoom.com' # ?¤ì „?¬ì
    endpoint = '/oauth2/token'
    url =  host + endpoint

    # 2. header ?°ì´??
    headers = {'Content-Type': 'application/json;charset=UTF-8'} # ì»¨í…ì¸ í???

    # 3. http POST ?”ì²­
    response = requests.post(url, headers=headers, json=data)

    # 4. ?‘ë‹µ ?íƒœ ì½”ë“œ?€ ?°ì´??ì¶œë ¥
    print('Code:', response.status_code)
    print('Header:', json.dumps({key: response.headers.get(key) for key in ['next-key', 'cont-yn', 'api-id']}, indent=4, ensure_ascii=False))
    print('Body:', json.dumps(response.json(), indent=4, ensure_ascii=False))  # JSON ?‘ë‹µ???Œì‹±?˜ì—¬ ì¶œë ¥
    return response
#---------------------?‘ê·¼? í° ì¶”ì¶œ--------------------------#
def get_token(response):
    data=response.json()
    token=data.get("token")
    return token
#---------------------? í° ìµœì´ˆ ë°œê¸‰ ---------------------------#
def toss_token():
    appkey = os.getenv("API_APPKEY")
    secret = os.getenv("API_SECRETKEY")
    params={'grant_type': 'client_credentials','appkey': appkey,'secretkey': secret}  # grant_type # ?±í‚¤# ?œí¬ë¦¿í‚¤

    # 2. API ?¤í–‰
    response_token=fn_au10001(data=params)
    token=get_token(response_token)
    return token
#---------------------DB ?…ë ¥--------------------------#
def conn_db():
    return psycopg2.connect(host="svc.sel3.cloudtype.app",port=31312,dbname="postgres",user="root",password="kiwoomapi2025",)
def pg_type_from_py(py_t):
    if py_t is bool:
        return "BOOLEAN"
    if py_t is int:
        return "BIGINT"
    if py_t is float:
        return "DOUBLE PRECISION"
    if py_t in (dict, list):
        return "JSONB"
    return "TEXT"  # ê·????„ë? TEXT'

def insert_db(result, table_name, conn):
    rows = result.get("orgn_frgnr_cont_trde_prst", [])
    if not rows:
        return 0

    # ì²?ë²ˆì§¸ ? íš¨ dict?ì„œ ì»¬ëŸ¼/?«ìì»¬ëŸ¼ ?ì •
    first = next((r for r in rows if isinstance(r, dict)), None)
    if not first:
        return 0
    fields = list(first.keys())
    numeric_cols = {c for c, v in first.items() if _is_floatable(v)}

    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in fields)
    col_idents   = sql.SQL(", ").join(sql.Identifier(c) for c in fields)
    q = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        col_idents,
        placeholders
    )

    inserted = 0
    with conn.cursor() as cur:
        for r in rows:
            if not isinstance(r, dict):
                continue

            vals = []
            for c in fields:
                v = r.get(c)
                if c in numeric_cols:
                    if _is_floatable(v):
                        vals.append(float(_norm(v)))  # "+6.97", "1,234", "--1578261" ì²˜ë¦¬
                    else:
                        vals.append(None)  # ?«ì ì»¬ëŸ¼???«ì ?„ë‹˜ ??NULL
                else:
                    # TEXT ì»¬ëŸ¼
                    if v is None or (isinstance(v, str) and v.strip() == ""):
                        vals.append(None)
                    else:
                        vals.append(str(v))
            cur.execute(q, vals)
            inserted += 1

    conn.commit()
    return inserted

def _norm(x):
    s = str(x).strip().replace(",", "")
    if s.startswith("+"): s = s[1:]      # +123 -> 123
    if s.startswith("--"): s = "-" + s[2:]  # --1578261 -> -1578261
    return s

def _is_floatable(v):
    try:
        float(_norm(v))
        return True
    except:
        return False
    
def create_table_by_schema(conn, table_name, result):
    rows = result.get("orgn_frgnr_cont_trde_prst", [])
    if not rows:
        return

    row0 = rows[0]
    field = list(row0.keys())
    field_type = ["DOUBLE PRECISION" if _is_floatable(v) else "TEXT"
                  for v in row0.values()]

    cols = [
        sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(t))
        for col, t in zip(field, field_type)
    ]
    q = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({});").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(cols)
    )
    with conn.cursor() as cur:
        cur.execute(q)
    conn.commit()


##########################################################################################################################################################################
#---------------------api ?¸ì¶œ ?¨ìˆ˜??--------------------------#
#---------------------?¸êµ­??ë§¤ìˆ˜ ë§¤ë„ ?„í™© api ?¸ì¶œ---------------------------#
def fn_ka10131(token, data, cont_yn='N', next_key=''):
    # 1. ?”ì²­??API URL
    host = 'https://mockapi.kiwoom.com' # ëª¨ì˜?¬ì
    # host = 'https://api.kiwoom.com' # ?¤ì „?¬ì
    endpoint = '/api/dostk/frgnistt'
    url =  host + endpoint

    # 2. header ?°ì´??
    headers = {
        'Content-Type': 'application/json;charset=UTF-8', # ì»¨í…ì¸ í???
        'authorization': f'Bearer {token}', # ?‘ê·¼? í°
        'cont-yn': cont_yn, # ?°ì†ì¡°íšŒ?¬ë?
        'next-key': next_key, # ?°ì†ì¡°íšŒ??
        'api-id': 'ka10131', # TRëª?
    }

    # 3. http POST ?”ì²­
    response = requests.post(url, headers=headers, json=data)

    # 4. ?‘ë‹µ ?íƒœ ì½”ë“œ?€ ?°ì´??ì¶œë ¥
    print('Code:', response.status_code)
    print('Header:', json.dumps({key: response.headers.get(key) for key in ['next-key', 'cont-yn', 'api-id']}, indent=4, ensure_ascii=False))
    print('Body:', json.dumps(response.json(), indent=4, ensure_ascii=False))  # JSON ?‘ë‹µ???Œì‹±?˜ì—¬ ì¶œë ¥
    return response

#---------------------api ê²°ê³¼ ?Œì‹±---------------------------#
def parse_data(response):
    data=response.json()
    return data

##########################################################################################################################################################################
#---------------------api ?¸ì¶œ ê²°ê³¼ ?¨ìˆ˜??--------------------------#
#---------------------?¸êµ­??ë§¤ìˆ˜ ë§¤ë„ ?„í™© api ?¸ì¶œ+ ?Œì‹± ê²°ê³¼---------------------------#
def get_ft(dt,strt_dt,end_dt,mrkt_tp,netslmt_tp,stk_inds_tp,amt_qty_tp,stex_tp,token):
        # ê¸°ê??¸êµ­?¸ì—°?ë§¤ë§¤í˜„?©ìš”ì²?
    MY_ACCESS_TOKEN = token
    params = {
        'dt': dt, # ê¸°ê°„ 1:ìµœê·¼?? 3:3?? 5:5?? 10:10?? 20:20?? 120:120?? 0:?œì‘?¼ì/ì¢…ë£Œ?¼ìë¡?ì¡°íšŒ
        'strt_dt': strt_dt, # ?œì‘?¼ì YYYYMMDD
        'end_dt': end_dt, # ì¢…ë£Œ?¼ì YYYYMMDD
        'mrkt_tp': mrkt_tp, # ?¥êµ¬ë¶?001:ì½”ìŠ¤?? 101:ì½”ìŠ¤??
        'netslmt_tp': netslmt_tp, # ?œë§¤?„ìˆ˜êµ¬ë¶„ 2:?œë§¤??ê³ ì •ê°?
        'stk_inds_tp': stk_inds_tp, # ì¢…ëª©?…ì¢…êµ¬ë¶„ 0:ì¢…ëª©(ì£¼ì‹),1:?…ì¢…
        'amt_qty_tp': amt_qty_tp, # ê¸ˆì•¡?˜ëŸ‰êµ¬ë¶„ 0:ê¸ˆì•¡, 1:?˜ëŸ‰
        'stex_tp': stex_tp, # ê±°ë˜?Œêµ¬ë¶?1:KRX, 2:NXT, 3:?µí•©
    }
    
    # 3. API ?¤í–‰
    response_ft=fn_ka10131(token=MY_ACCESS_TOKEN, data=params,cont_yn='Y', next_key='nextkey..')
    json_ft=parse_data(response_ft)

    # next-key, cont-yn ê°’ì´ ?ˆì„ ê²½ìš°
    # fn_ka10131(token=MY_ACCESS_TOKEN, data=params, cont_yn='Y', next_key='nextkey..')
    return json_ft


#---------------------ìµœì¢…?ìœ¼ë¡??˜ì§‘???°ì´??api ???¬ê¸°?ë‹¤ê°€ ?£ì–´????>DB?£ëŠ” ê²ƒê¹Œì§€ ??êµ¬í˜„ ---------------------------#
def run_schedule(token,conn):
    today_date=date.today()
    ft=get_ft('1','','' ,'001','2','0','0','1',token)
    ft_table=True
    while ft_table:
        create_table_by_schema(conn,"fn_ka10131",ft)
        ft_table=False

    insert_db(ft,"fn_ka10131",conn)

#---------------------?¤í–‰---------------------------#
token=toss_token()#? í° ìµœì´ˆ 1??ë°œê¸‰
conn=conn_db()
schedule.every(1).minutes.do(lambda: (lambda now=datetime.now(): run_schedule(token,conn) if (now.weekday()<5 and (9,0,0) <= (now.hour,now.minute,now.second) <= (15,30,0)) else None)())
while True:
    schedule.run_pending()
    time.sleep(1)
# ?‰ì¼ 9?œë???15:30ë¶„ê¹Œì§€ ë§¤ë¶„ë§ˆë‹¤ ?¤í–‰
