import requests

url = "https://www.google.com/maps/rpc/listugcposts"

querystring = {"authuser":"0","hl":"en","pb":"!1m6!1s0x89c3ca9c11f90c25:0x6cc8dba851799f09!6m4!4m1!1e1!4m1!1e3!2m2!1i20!2s!5m2!1sStliaIi6EPWA9u8PwLTBwAE!7e81!8m9!2b1!3b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!11m0!13m1!1e3"}

payload = ""
headers = {
    "^accept": "*/*^",
    "^accept-language": "en-US,en;q=0.9^",
    "^cache-control": "no-cache^",
    "^downlink": "1.4^",
    "^pragma": "no-cache^",
    "^priority": "u=1, i^",
    "^referer": "https://www.google.com/^",
    "^rtt": "150^",
    "^sec-ch-ua": "^\^Not"
}

response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
with open("response.html", "w", encoding="utf-8") as file:
    file.write(response.text)
