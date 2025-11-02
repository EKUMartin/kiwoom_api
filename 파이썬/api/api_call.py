import os, json, requests
from datetime import datetime,date, time as dtime
from pydantic import BaseModel
import schedule
import time 
import psycopg2
from psycopg2 import sql
#---------------------?�근?�큰 발급---------------------------#
def fn_au10001(data):
    # 1. ?�청??API URL
    host = 'https://mockapi.kiwoom.com' # 모의?�자
    # host = 'https://api.kiwoom.com' # ?�전?�자
    endpoint = '/oauth2/token'
    url =  host + endpoint

    # 2. header ?�이??
    headers = {'Content-Type': 'application/json;charset=UTF-8'} # 컨텐츠�???

    # 3. http POST ?�청
    response = requests.post(url, headers=headers, json=data)

    # 4. ?�답 ?�태 코드?� ?�이??출력
    print('Code:', response.status_code)
    print('Header:', json.dumps({key: response.headers.get(key) for key in ['next-key', 'cont-yn', 'api-id']}, indent=4, ensure_ascii=False))
    print('Body:', json.dumps(response.json(), indent=4, ensure_ascii=False))  # JSON ?�답???�싱?�여 출력
    return response
#---------------------?�근?�큰 추출--------------------------#
def get_token(response):
    data=response.json()
    token=data.get("token")
    return token
#---------------------?�큰 최초 발급 ---------------------------#
def toss_token():
    appkey = os.getenv("API_APPKEY")
    secret = os.getenv("API_SECRETKEY")
    params={'grant_type': 'client_credentials','appkey': appkey,'secretkey': secret}  # grant_type # ?�키# ?�크릿키

    # 2. API ?�행
    response_token=fn_au10001(data=params)
    token=get_token(response_token)
    return token
#---------------------DB ?�력--------------------------#
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
    return "TEXT"  # �????��? TEXT'

def insert_db(result, table_name, conn):
    rows = result.get("orgn_frgnr_cont_trde_prst", [])
    if not rows:
        return 0

    # �?번째 ?�효 dict?�서 컬럼/?�자컬럼 ?�정
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
                        vals.append(float(_norm(v)))  # "+6.97", "1,234", "--1578261" 처리
                    else:
                        vals.append(None)  # ?�자 컬럼???�자 ?�님 ??NULL
                else:
                    # TEXT 컬럼
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
    cols.append(sql.SQL("{} {}").format(sql.Identifier("added_time"), sql.SQL("TIMESTAMPTZ DEFAULT now()")))
    q = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({});").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(cols)
    )
    with conn.cursor() as cur:
        cur.execute(q)
    conn.commit()


##########################################################################################################################################################################
#---------------------api ?�출 ?�수??--------------------------#
#---------------------?�국??매수 매도 ?�황 api ?�출---------------------------#
def fn_ka10131(token, data, cont_yn='N', next_key=''):
    # 1. ?�청??API URL
    host = 'https://mockapi.kiwoom.com' # 모의?�자
    # host = 'https://api.kiwoom.com' # ?�전?�자
    endpoint = '/api/dostk/frgnistt'
    url =  host + endpoint

    # 2. header ?�이??
    headers = {
        'Content-Type': 'application/json;charset=UTF-8', # 컨텐츠�???
        'authorization': f'Bearer {token}', # ?�근?�큰
        'cont-yn': cont_yn, # ?�속조회?��?
        'next-key': next_key, # ?�속조회??
        'api-id': 'ka10131', # TR�?
    }

    # 3. http POST ?�청
    response = requests.post(url, headers=headers, json=data)

    # 4. ?�답 ?�태 코드?� ?�이??출력
    print('Code:', response.status_code)
    print('Header:', json.dumps({key: response.headers.get(key) for key in ['next-key', 'cont-yn', 'api-id']}, indent=4, ensure_ascii=False))
    print('Body:', json.dumps(response.json(), indent=4, ensure_ascii=False))  # JSON ?�답???�싱?�여 출력
    return response

#---------------------api 결과 ?�싱---------------------------#
def parse_data(response):
    data=response.json()
    return data

##########################################################################################################################################################################
#---------------------api ?�출 결과 ?�수??--------------------------#
#---------------------?�국??매수 매도 ?�황 api ?�출+ ?�싱 결과---------------------------#
def get_ft(dt,strt_dt,end_dt,mrkt_tp,netslmt_tp,stk_inds_tp,amt_qty_tp,stex_tp,token):
        # 기�??�국?�연?�매매현?�요�?
    MY_ACCESS_TOKEN = token
    params = {
        'dt': dt, # 기간 1:최근?? 3:3?? 5:5?? 10:10?? 20:20?? 120:120?? 0:?�작?�자/종료?�자�?조회
        'strt_dt': strt_dt, # ?�작?�자 YYYYMMDD
        'end_dt': end_dt, # 종료?�자 YYYYMMDD
        'mrkt_tp': mrkt_tp, # ?�구�?001:코스?? 101:코스??
        'netslmt_tp': netslmt_tp, # ?�매?�수구분 2:?�매??고정�?
        'stk_inds_tp': stk_inds_tp, # 종목?�종구분 0:종목(주식),1:?�종
        'amt_qty_tp': amt_qty_tp, # 금액?�량구분 0:금액, 1:?�량
        'stex_tp': stex_tp, # 거래?�구�?1:KRX, 2:NXT, 3:?�합
    }
    
    # 3. API ?�행
    response_ft=fn_ka10131(token=MY_ACCESS_TOKEN, data=params,cont_yn='Y', next_key='nextkey..')
    json_ft=parse_data(response_ft)

    # next-key, cont-yn 값이 ?�을 경우
    # fn_ka10131(token=MY_ACCESS_TOKEN, data=params, cont_yn='Y', next_key='nextkey..')
    return json_ft


#---------------------최종?�으�??�집???�이??api ???�기?�다가 ?�어????>DB?�는 것까지 ??구현 ---------------------------#
def run_schedule(token,conn):
    today_date=date.today()
    ft=get_ft('1','','' ,'001','2','0','0','1',token)
    ft_table=True
    while ft_table:
        create_table_by_schema(conn,"fn_ka10131",ft)
        ft_table=False

    insert_db(ft,"fn_ka10131",conn)

#---------------------?�행---------------------------#
token=toss_token()#?�큰 최초 1??발급
conn=conn_db()
schedule.every(1).minutes.do(lambda: (lambda now=datetime.now(): run_schedule(token,conn) if (now.weekday()<5 and (9,0,0) <= (now.hour,now.minute,now.second) <= (15,30,0)) else None)())
while True:
    schedule.run_pending()
    time.sleep(1)
# ?�일 9?��???15:30분까지 매분마다 ?�행
