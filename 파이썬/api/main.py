from fastapi import FastAPI
import threading, yaml
from api_call import loop, get_state

app = FastAPI()

# 경로 주의: main.py와 endpoints.yaml이 같은 폴더면 "endpoints.yaml"로.
# 레포 구조가 collector/main.py 이고 파일이 collector/endpoints.yaml이면 아래처럼 유지.
with open("endpoints.yaml","r",encoding="utf-8") as f:
    CONF = yaml.safe_load(f)

threading.Thread(target=loop, args=(CONF,), daemon=True).start()

@app.get("/health")
def health():
    st = get_state()
    return {"status":"ok", **st, "jobs": [j["id"] for j in CONF["jobs"]]}
