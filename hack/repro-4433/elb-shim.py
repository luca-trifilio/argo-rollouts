"""ALB target-admission shim in front of Floci's ELBv2 API.

Floci routes to a target the moment it is registered. A real ALB does not: a new
target stays `initial` and gets no traffic until it passes its initial health checks.
This proxy models that. Everything is passed through unchanged except:
  RegisterTargets      -> acknowledged, but the target is held back from Floci until it
                          passes ADMIT_CHECKS health checks (default: the target group's
                          HealthyThresholdCount), one per HealthCheckIntervalSeconds.
  DescribeTargetHealth -> held-back targets are reported as `initial`, as AWS does.
  DeregisterTargets    -> also drops held-back targets.
"""
import http.client, os, re, sys, threading, time, urllib.parse, urllib.request, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UP = ("127.0.0.1", 4566)
ADMIT_CHECKS = int(os.environ.get("ADMIT_CHECKS", "0"))  # 0 = use the TG's HealthyThresholdCount
NS = "http://elasticloadbalancing.amazonaws.com/doc/2015-12-01/"
AUTH = "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/elasticloadbalancing/aws4_request, SignedHeaders=host, Signature=x"
lock = threading.Lock()
pending = {}  # (tg, id, port) -> {"next": t, "ok": n}
tgcfg = {}

def log(*a):
    t = time.time(); print(time.strftime("%H:%M:%S", time.gmtime(t)) + ".%03d" % (t % 1 * 1000), *a, flush=True)

def upstream(body, headers=None):
    c = http.client.HTTPConnection(*UP, timeout=30)
    h = headers or {"Content-Type": "application/x-www-form-urlencoded", "Authorization": AUTH}
    c.request("POST", "/", body, h); r = c.getresponse(); data = r.read(); c.close()
    return r.status, r.getheaders(), data

def call(**params):
    params["Version"] = "2015-12-01"
    return upstream(urllib.parse.urlencode(params).encode())

def cfg(tg):
    if tg not in tgcfg:
        _, _, x = call(Action="DescribeTargetGroups", **{"TargetGroupArns.member.1": tg}); x = x.decode()
        g = lambda k, d: (re.search("<%s>([^<]*)</%s>" % (k, k), x) or [None, d])[1]
        tgcfg[tg] = dict(path=g("HealthCheckPath", "/"), interval=int(g("HealthCheckIntervalSeconds", 30)),
                         threshold=int(g("HealthyThresholdCount", 5)), timeout=int(g("HealthCheckTimeoutSeconds", 5)))
    return tgcfg[tg]

def targets(q):
    out = []; i = 1
    while "Targets.member.%d.Id" % i in q:
        out.append((q["Targets.member.%d.Id" % i], q.get("Targets.member.%d.Port" % i, ""))); i += 1
    return out

def ok(action):
    return ('<%sResponse xmlns="%s"><%sResult/><ResponseMetadata><RequestId>%s</RequestId></ResponseMetadata></%sResponse>'
            % (action, NS, action, uuid.uuid4(), action)).encode()

def prober():
    while True:
        time.sleep(0.25); now = time.time()
        with lock: due = [(k, v) for k, v in pending.items() if v["next"] <= now]
        for (tg, ip, port), st in due:
            c = cfg(tg)
            try:
                good = urllib.request.urlopen("http://%s:%s%s" % (ip, port, c["path"]), timeout=c["timeout"]).status == 200
            except Exception:
                good = False
            st["ok"] = st["ok"] + 1 if good else 0; st["next"] = now + c["interval"]
            need = ADMIT_CHECKS or c["threshold"]
            log("check %s:%s %s (%d/%d) tg=%s" % (ip, port, "pass" if good else "FAIL", st["ok"], need, tg[-16:]))
            if st["ok"] >= need:
                with lock:
                    if pending.pop((tg, ip, port), None) is None: continue
                call(Action="RegisterTargets", TargetGroupArn=tg, **{"Targets.member.1.Id": ip, "Targets.member.1.Port": port})
                log("ADMITTED %s:%s -> in rotation tg=%s" % (ip, port, tg[-16:]))

class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def reply(self, status, data, ctype="text/xml"):
        self.send_response(status); self.send_header("Content-Type", ctype); self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)
    def passthru(self, method, body):
        c = http.client.HTTPConnection(*UP, timeout=60)
        c.request(method, self.path, body, {k: v for k, v in self.headers.items() if k.lower() not in ("host", "connection")})
        r = c.getresponse(); data = r.read(); c.close()
        self.send_response(r.status)
        for k, v in r.getheaders():
            if k.lower() not in ("transfer-encoding", "content-length", "connection"): self.send_header(k, v)
        self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)
    def do_GET(self): self.passthru("GET", None)
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        q = {}
        if b"Version=2015-12-01" in body:
            q = {k: v[0] for k, v in urllib.parse.parse_qs(body.decode(), keep_blank_values=True).items()}
        a = q.get("Action"); tg = q.get("TargetGroupArn")
        if a == "RegisterTargets":
            c = cfg(tg)
            with lock:
                for ip, port in targets(q):
                    pending.setdefault((tg, ip, port), {"next": time.time() + c["interval"], "ok": 0})
            log("REGISTER (held, initial) %s tg=%s" % (",".join(i for i, _ in targets(q)), tg[-16:]))
            return self.reply(200, ok(a))
        if a == "DeregisterTargets":
            with lock: held = [t for t in targets(q) if pending.pop((tg,) + t, None)]
            log("DEREGISTER %s tg=%s%s" % (",".join(i for i, _ in targets(q)), tg[-16:], " (%d were held)" % len(held) if held else ""))
            s, _, data = upstream(body, {"Content-Type": self.headers.get("Content-Type", ""), "Authorization": self.headers.get("Authorization", AUTH)})
            return self.reply(200, ok(a)) if (s != 200 and held) else self.reply(s, data)
        if a == "DescribeTargetHealth":
            s, _, data = upstream(body, {"Content-Type": self.headers.get("Content-Type", ""), "Authorization": self.headers.get("Authorization", AUTH)})
            want = set(targets(q))
            with lock: held = [(i, p) for (t, i, p) in pending if t == tg and (not want or (i, p) in want)]
            if s == 200 and held:
                x = data.decode()
                m = "".join("<member><Target><Id>%s</Id><Port>%s</Port></Target><HealthCheckPort>%s</HealthCheckPort><TargetHealth><State>initial</State>"
                            "<Reason>Elb.RegistrationInProgress</Reason><Description>Target registration is in progress</Description></TargetHealth></member>" % (i, p, p) for i, p in held)
                if "</TargetHealthDescriptions>" in x: x = x.replace("</TargetHealthDescriptions>", m + "</TargetHealthDescriptions>", 1)
                else: x = re.sub(r"<TargetHealthDescriptions\s*/>", "<TargetHealthDescriptions>" + m + "</TargetHealthDescriptions>", x, 1)
                data = x.encode()
            return self.reply(s, data)
        self.passthru("POST", body)

threading.Thread(target=prober, daemon=True).start()
log("elb-shim listening on :4567 -> %s:%d (ADMIT_CHECKS=%s)" % (UP + (ADMIT_CHECKS or "tg threshold",)))
ThreadingHTTPServer(("0.0.0.0", 4567), H).serve_forever()
