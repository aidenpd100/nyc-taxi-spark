import os, sys, urllib.request
import socket, time
socket.setdefaulttimeout(60)

BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"   # verify on the TLC page
SERVICES = ["yellow", "fhvhv"]

def months(start, end):
    y, m = start
    while (y, m) <= end:
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)

def progress(count, block, total):
    done = count * block
    if count % 400 == 0:
        pct = f"{100*done/total:.0f}%" if total > 0 else "?"
        print(f"   {done/1e6:7.1f} MB  {pct}", flush=True)

def main(start=(2024, 1), end=(2025, 12)):
    for svc in SERVICES:
        os.makedirs(f"data/raw/{svc}", exist_ok=True)
        for y, m in months(start, end):
            name = f"{svc}_tripdata_{y}-{m:02d}.parquet"
            dest = f"data/raw/{svc}/{name}"
            if os.path.exists(dest):
                continue
            try:
                print("start", name, flush=True)
                urllib.request.urlretrieve(f"{BASE}/{name}", dest + ".part", reporthook=progress)
                os.rename(dest + ".part", dest)
                print("ok  ", name)
            except Exception as e:
                print("FAIL", name, e, file=sys.stderr)

if __name__ == "__main__":
    main()