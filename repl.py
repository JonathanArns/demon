#!/bin/python

import requests
import sys

query_mode = "weak"

addr = None
addrs_to_try = [
    "localhost:80",
    "localhost:8000",
    "localhost:8080",
]

for item in addrs_to_try:
    try:
        resp = requests.get(f"http://{item}")
        if resp.text == "Hello world!":
            addr = item
            break
    except:
        pass
if addr == None:
    print(f"could not find locally running demon\nconnect with `addr <address>`")
else:
    print(f"found demon at `{addr}`\nswitch to different node with `addr <address>`")


while True:
    line = str(input("> ")).strip()
    parts = line.split(" ", 1)
    command = parts[0]
    args = parts[1] if len(parts) > 1 else ""
    query = command

    if "exit" == command:
        break

    elif "addr" == command:
        try:
            requests.get(f"http://{args}")
            if resp.text == "Hello world!":
                addr = args
                print("connected")
            else:
                print(f"wrong response: {resp.text}")
        except:
            print("could not connect")
        continue

    elif "weak" == command or "w" == command:
        query_mode = "weak"
        if args:
            query = args
        else:
            continue

    elif "strong" == command or "s" == command:
        query_mode = "strong"
        if args:
            query = args
        else:
            continue

    if not addr:
        print(f"not connected. connect with `addr <address>`")
    resp = requests.post(f"http://{addr}/{query_mode}", data=query)
    print(resp.text)

print("Goodbye.")
