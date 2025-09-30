import requests
import json
import subprocess
import schedule
import time
from datetime import datetime, timedelta

#---------------------접근토큰 발급---------------------------#
def fn_au10001(data):
	# 1. 요청할 API URL
	host = 'https://mockapi.kiwoom.com' # 모의투자
	# host = 'https://api.kiwoom.com' # 실전투자
	endpoint = '/oauth2/token'
	url =  host + endpoint

	# 2. header 데이터
	headers = {
		'Content-Type': 'application/json;charset=UTF-8', # 컨텐츠타입
	}

	# 3. http POST 요청
	response = requests.post(url, headers=headers, json=data)

	# 4. 응답 상태 코드와 데이터 출력
	print('Code:', response.status_code)
	print('Header:', json.dumps({key: response.headers.get(key) for key in ['next-key', 'cont-yn', 'api-id']}, indent=4, ensure_ascii=False))
	print('Body:', json.dumps(response.json(), indent=4, ensure_ascii=False))  # JSON 응답을 파싱하여 출력
	return response
#---------------------접근토큰 추출--------------------------#
def get_token(response):
	data=response.json()
	token=data.get("token")
	return token


#---------------------외국인 매수 매도 현황 api 호출---------------------------#
def fn_ka10131(token, data, cont_yn='N', next_key=''):
	# 1. 요청할 API URL
	host = 'https://mockapi.kiwoom.com' # 모의투자
	# host = 'https://api.kiwoom.com' # 실전투자
	endpoint = '/api/dostk/frgnistt'
	url =  host + endpoint

	# 2. header 데이터
	headers = {
		'Content-Type': 'application/json;charset=UTF-8', # 컨텐츠타입
		'authorization': f'Bearer {token}', # 접근토큰
		'cont-yn': cont_yn, # 연속조회여부
		'next-key': next_key, # 연속조회키
		'api-id': 'ka10131', # TR명
	}

	# 3. http POST 요청
	response = requests.post(url, headers=headers, json=data)

	# 4. 응답 상태 코드와 데이터 출력
	print('Code:', response.status_code)
	print('Header:', json.dumps({key: response.headers.get(key) for key in ['next-key', 'cont-yn', 'api-id']}, indent=4, ensure_ascii=False))
	print('Body:', json.dumps(response.json(), indent=4, ensure_ascii=False))  # JSON 응답을 파싱하여 출력
	return response

#---------------------api 결과 파싱---------------------------#
def parse_data(response):
	data=response.json()
	return data
#---------------------외국인 매수 매도 현황 api 호출+ 파싱 결과---------------------------#
def get_ft(dt,strt_dt,end_dt,mrkt_tp,netslmt_tp,stk_inds_tp,amt_qty_tp,stex_tp,token):
		# 기관외국인연속매매현황요청
	MY_ACCESS_TOKEN = token
	params = {
		'dt': dt, # 기간 1:최근일, 3:3일, 5:5일, 10:10일, 20:20일, 120:120일, 0:시작일자/종료일자로 조회
		'strt_dt': strt_dt, # 시작일자 YYYYMMDD
		'end_dt': end_dt, # 종료일자 YYYYMMDD
		'mrkt_tp': mrkt_tp, # 장구분 001:코스피, 101:코스닥
		'netslmt_tp': netslmt_tp, # 순매도수구분 2:순매수(고정값)
		'stk_inds_tp': stk_inds_tp, # 종목업종구분 0:종목(주식),1:업종
		'amt_qty_tp': amt_qty_tp, # 금액수량구분 0:금액, 1:수량
		'stex_tp': stex_tp, # 거래소구분 1:KRX, 2:NXT, 3:통합
	}

	# 3. API 실행
	response_ft=fn_ka10131(token=MY_ACCESS_TOKEN, data=params)
	json_ft=parse_data(response_ft)

	# next-key, cont-yn 값이 있을 경우
	# fn_ka10131(token=MY_ACCESS_TOKEN, data=params, cont_yn='Y', next_key='nextkey..')
	return json_ft

#---------------------최종적으로 수집할 데이터 api 다 여기에다가 넣어야 함->DB넣는 것까지 다 구현 ---------------------------#
def run_schedule():

	params = {
		'grant_type': 'client_credentials',  # grant_type
		'appkey': 'EJUvIP-gLXzixSvHm6noCiAs7Vg-1w0PfgNL5cj2f_Q',  # 앱키
		'secretkey': 'Y_Z84ClM-8cdjdqzgGpvAcBDAZhw_O20rXEpAMYxRq0',  # 시크릿키
	}

	# 2. API 실행
	response_token=fn_au10001(data=params)
	token=get_token(response_token)
	ft=get_ft('1','','' ,'001','2','0','0','1',token)


#---------------------실행---------------------------#
if __name__ == '__main__':
	while True:
		schedule.every(5).minutes.do(lambda: (lambda now=datetime.now(): run_schedule() if (now.weekday()<5 and (9,0,0) <= (now.hour,now.minute,now.second) <= (15,30,0)) else None)())