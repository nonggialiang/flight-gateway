#!/usr/bin/env python3
"""FG M1 e2e：pyarrow 原始 FlightClient（手工编码 Flight SQL protobuf）+ grpcio PollFlightInfo。"""
import base64
import os
import struct
import sys
import time

# localhost 流量不走系统代理（ClashX 等会劫持 gRPC）
for k in ("http_proxy", "https_proxy", "all_proxy", "grpc_proxy",
          "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "GRPC_PROXY"):
    os.environ.pop(k, None)
os.environ["no_proxy"] = "localhost,127.0.0.1"
os.environ["NO_PROXY"] = "localhost,127.0.0.1"

import pyarrow
import pyarrow.flight as fl

GW = b"grpc://localhost:32010"
USER, PASSWORD = "fg", "fg"
TYPE_QUERY = "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"

# ---------------- protobuf 手工编码 ----------------

def varint(n):
    out = b""
    while True:
        b = n & 0x7F
        n >>= 7
        out += bytes([b | (0x80 if n else 0)])
        if not n:
            return out

def tag(field, wire):
    return varint((field << 3) | wire)

def f_bytes(field, data):
    return tag(field, 2) + varint(len(data)) + data

def f_string(field, s):
    return f_bytes(field, s.encode())

def walk(data):
    """yield (field_no, wire, value)"""
    i = 0
    while i < len(data):
        n = 0
        shift = 0
        while True:
            b = data[i]
            i += 1
            n |= (b & 0x7F) << shift
            shift += 7
            if not b & 0x80:
                break
        field, wire = n >> 3, n & 7
        if wire == 0:
            v = 0
            shift = 0
            while True:
                b = data[i]
                i += 1
                v |= (b & 0x7F) << shift
                shift += 7
                if not b & 0x80:
                    break
            yield field, wire, v
        elif wire == 2:
            ln = 0
            shift = 0
            while True:
                b = data[i]
                i += 1
                ln |= (b & 0x7F) << shift
                shift += 7
                if not b & 0x80:
                    break
            yield field, wire, data[i:i + ln]
            i += ln
        else:
            raise ValueError(f"wire {wire} unsupported")

def command_descriptor(sql):
    """Any{CommandStatementQuery{string query}} → FlightDescriptor"""
    any_msg = f_string(1, TYPE_QUERY) + f_bytes(2, f_string(1, sql))
    return fl.FlightDescriptor.for_command(any_msg)

# ---------------- 客户端 ----------------

class Client:
    """FlightClient + Basic token（grpcio 双消息握手：username、password → bearer）。"""

    def __init__(self):
        import grpc
        self.fc = fl.FlightClient(GW)
        ch = grpc.insecure_channel("localhost:32010")
        hs = ch.stream_stream(
            "/arrow.flight.protocol.FlightService/Handshake",
            request_serializer=lambda b: b,
            response_deserializer=lambda b: b)

        def gen():
            # HandshakeRequest{payload=2} = Flight.BasicAuth{username=2, password=3}（单条）
            basic_auth = f_string(2, USER) + f_string(3, PASSWORD)
            yield f_bytes(2, basic_auth)

        token = None
        for resp in hs(gen()):
            for fno, w, v in walk(resp):
                if fno == 2:
                    token = v
        assert token, "handshake returned no token"
        # arrow-java auth1：二进制头 Auth-Token-bin（非 authorization Bearer）
        self.headers = [(b"auth-token-bin", token)]
        self.opts = fl.FlightCallOptions(headers=self.headers, timeout=600)

    def get_flight_info(self, descr):
        return self.fc.get_flight_info(descr, options=self.opts)

    def do_get(self, ticket):
        return self.fc.do_get(ticket, options=self.opts)

    def do_action(self, action):
        return self.fc.do_action(action, options=self.opts)


def client():
    return Client()

# ---------------- 用例 ----------------

def legacy_get_flight_info(c, sql, expect_rows):
    info = c.get_flight_info(command_descriptor(sql))
    qid = info.app_metadata.decode() if info.app_metadata else "?"
    print(f"[legacy] endpoints={len(info.endpoints)} queryId={qid[:8]}...")
    total = 0
    for ep in info.endpoints:
        reader = c.do_get(ep.ticket)
        table = reader.read_all()
        total += table.num_rows
        print(f"[legacy] part rows={table.num_rows} cols={table.column_names}")
    assert total == expect_rows, f"rows {total} != {expect_rows}"
    print("[legacy] PASS")
    return qid

def poll_flight_info(c, sql, expect_rows, max_polls=90):
    """pyarrow FlightClient 无 poll API → grpcio 直打 FlightService/PollFlightInfo。"""
    import grpc

    ch = grpc.insecure_channel("localhost:32010")
    # 复用 Client 握手拿到的 token（auth-token-bin 二进制头）
    token = c.headers[0][1]
    md = (("auth-token-bin", token),)

    # 2) PollFlightInfo：请求体直接是 FlightDescriptor{descriptor_type=1:CMD(2), command=2:Any}
    any_msg = f_string(1, TYPE_QUERY) + f_bytes(2, f_string(1, sql))
    descriptor = (
        varint((1 << 3) | 0) + varint(2)  # descriptor_type = CMD = 2
        + f_bytes(2, any_msg))
    poll = ch.unary_unary("/arrow.flight.protocol.FlightService/PollFlightInfo",
                          request_serializer=lambda x: x, response_deserializer=lambda x: x)
    info_bytes = None
    for i in range(max_polls):
        raw = poll(descriptor, metadata=md)
        fields = {f: v for f, w, v in walk(raw) if w == 2}
        fields_all = [f for f, w, v in walk(raw)]
        if 2 not in fields_all:  # flight_descriptor unset → 终态
            info_bytes = fields[1]
            print(f"[poll] terminal after {i + 1} polls")
            break
        print(f"[poll {i}] pending")
        time.sleep(1)
    assert info_bytes, "not terminal in time"

    endpoints, records, qid = [], None, "?"
    for fno, w, v in walk(info_bytes):
        if fno == 3 and w == 2:
            endpoints.append(v)
        elif fno == 4 and w == 0:
            records = v
        elif fno == 7 and w == 2:
            qid = v.decode()
    print(f"[poll] endpoints={len(endpoints)} totalRecords={records} queryId={qid[:8]}...")

    # 3) 取 endpoints 的票（FlightEndpoint{ticket=1{data=1}}）逐分片 DoGet
    total = 0
    for epb in endpoints:
        ticket_bytes = None
        for fno, w, v in walk(epb):
            if fno == 1:  # Ticket{data=1}：再解一层取 data
                for f2, w2, v2 in walk(v):
                    if f2 == 1:
                        ticket_bytes = v2
        t = fl.Ticket(ticket_bytes)
        reader = c.do_get(t)
        table = reader.read_all()
        total += table.num_rows
        print(f"[poll] part rows={table.num_rows} cols={table.column_names}")
    assert total == expect_rows, f"rows {total} != {expect_rows}"
    print("[poll] PASS")
    return qid


def parse_endpoint(epb):
    """FlightEndpoint{ticket=1{data=1}, location=2{uri=1}} → (ticket_data|None, [urls])"""
    ticket = None; locations = []
    for fno, w, v in walk(epb):
        if fno == 1:
            for f2, w2, v2 in walk(v):
                if f2 == 1:
                    ticket = v2
        elif fno == 2:
            for f2, w2, v2 in walk(v):
                if f2 == 1:
                    locations.append(v2.decode())
    return ticket, locations


def poll_terminal(c, sql, max_polls=90):
    """轮询至终态，返回 (endpoints_raw_bytes, records, qid)。"""
    import grpc
    ch = grpc.insecure_channel("localhost:32010")
    md = (("auth-token-bin", c.headers[0][1]),)
    any_msg = f_string(1, TYPE_QUERY) + f_bytes(2, f_string(1, sql))
    descriptor = varint((1 << 3) | 0) + varint(2) + f_bytes(2, any_msg)
    poll = ch.unary_unary("/arrow.flight.protocol.FlightService/PollFlightInfo",
                          request_serializer=lambda x: x, response_deserializer=lambda x: x)
    info_bytes = None
    for i in range(max_polls):
        raw = poll(descriptor, metadata=md)
        fields = {f: v for f, w, v in walk(raw) if w == 2}
        if 2 not in [f for f, w, v in walk(raw)]:
            info_bytes = fields[1]
            print(f"[https] terminal after {i + 1} polls")
            break
        time.sleep(1)
    assert info_bytes, "not terminal in time"
    endpoints, records, qid = [], None, "?"
    for fno, w, v in walk(info_bytes):
        if fno == 3 and w == 2:
            endpoints.append(v)
        elif fno == 4 and w == 0:
            records = v
        elif fno == 7 and w == 2:
            qid = v.decode()
    return endpoints, records, qid


def https_presign(c, sql, expect_rows, do_renew=False):
    import urllib.request
    import pyarrow.ipc
    endpoints, records, qid = poll_terminal(c, sql)
    total = 0
    first_ep = endpoints[0] if endpoints else None
    for epb in endpoints:
        ticket, locations = parse_endpoint(epb)
        assert ticket is None, f"https endpoint must carry EMPTY ticket, got {ticket!r}"
        assert locations, "https endpoint must carry presigned location"
        url = locations[0]
        with urllib.request.urlopen(url) as resp:
            payload = resp.read()
        table = pyarrow.ipc.open_stream(payload).read_all()
        total += table.num_rows
        print(f"[https] GET presigned -> rows={table.num_rows} cols={table.column_names} bytes={len(payload)}")
    assert total == expect_rows, f"rows {total} != {expect_rows}"
    print("[https] PASS")
    if do_renew and first_ep is not None:
        body = f_bytes(1, first_ep)
        results = list(c.do_action(fl.Action("RenewFlightEndpoint", body)))
        assert results, "renew returned no result"
        raw = results[0].body.to_pybytes()
        _, urls = parse_endpoint(raw)
        assert urls, "renewed endpoint has no location"
        with urllib.request.urlopen(urls[0]) as resp:
            table = pyarrow.ipc.open_stream(resp.read()).read_all()
        print(f"[renew] new presigned URL -> rows={table.num_rows} cols={table.column_names}")
        assert table.num_rows > 0
        print("[renew] PASS")
    return qid

def cancel_inflight(c, sql):
    info = c.get_flight_info(command_descriptor(sql))
    qid = info.app_metadata.decode()
    print(f"[cancel] queryId={qid[:8]}... submitted")
    time.sleep(3)  # 确保在途
    # CancelFlightInfo 为 Flight SQL action（类型名 CancelFlightInfo）：
    #   body = CancelFlightInfoRequest{ FlightInfo info = 1 }，Result = CancelStatus(varint)
    body = f_bytes(1, info.serialize())
    results = list(c.do_action(fl.Action("CancelFlightInfo", body)))
    status = None
    if results:
        raw = results[0].body.to_pybytes()
        try:
            for fno, w, v in walk(raw):
                if fno == 1 and w == 0:
                    status = v
        except ValueError:
            status = raw[0]  # 容错：裸 varint
    print(f"[cancel] CancelStatus={status} (0=UNSPECIFIED,1=CANCELLED,2=NOT_CANCELLABLE)")
    assert status == 1, f"expected CANCELLED(1), got {status}"
    print("[cancel] PASS")
    return qid


def https_order_by(c):
    """H4（https 侧）：ORDER BY → 写端 coalesce(1) 单 part → 终态恰 1 个 presigned endpoint，行序保持。"""
    import urllib.request
    import pyarrow.ipc
    endpoints, records, qid = poll_terminal(c, "SELECT id FROM range(5000) ORDER BY id")
    assert len(endpoints) == 1, f"H4 violated: {len(endpoints)} endpoints for ORDER BY in https mode"
    ticket, locations = parse_endpoint(endpoints[0])
    assert ticket is None and len(locations) == 1
    with urllib.request.urlopen(locations[0]) as resp:
        table = pyarrow.ipc.open_stream(resp.read()).read_all()
    rows = table.column("id").to_pylist()
    assert len(rows) == 5000 and rows == sorted(rows), "order must be preserved"
    print(f"[https-order] single endpoint, 5000 rows in order: PASS")
    from minio import Minio
    import json
    m = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
    mani = json.loads(m.get_object("fg-results", f"results/spark/fg/{qid}/manifest.json").read())
    assert len(mani["parts"]) == 1 and mani["parts"][0]["recordCount"] == 5000, \
        "write side must coalesce to a single part"
    print("[https-order] write-side coalesce(1): PASS")
    return qid

if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "legacy"
    c = client()
    if which == "legacy":
        legacy_get_flight_info(c, "SELECT id, id * 2 AS dbl, concat('v-', cast(id as STRING)) AS s FROM range(1000)", 1000)
    elif which == "poll":
        poll_flight_info(c, "SELECT id FROM range(1000)", 1000)
    elif which == "poll-multi":
        poll_flight_info(c, "SELECT id FROM range(5000) ORDER BY id", 5000)
    elif which == "https":
        https_presign(c, "SELECT id, id * 3 AS t FROM range(3000)", 3000, do_renew=True)
    elif which == "https-order":
        https_order_by(c)
    elif which == "cancel":
        cancel_inflight(c, "SELECT a.id FROM range(400000000) a JOIN range(500) b ON a.id % 500 = b.id")
    else:
        raise SystemExit(f"unknown case {which}")
