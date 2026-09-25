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
    """FlightClient + auth2 Bearer（网关 auth2-only，FgBearerTokenAuthenticator 签发）。
    pyarrow 的 authenticate_basic_token 返回 (authorization, Bearer <token>) 头对，
    需自行经 FlightCallOptions 附到每个调用；grpcio 直打（PollFlightInfo）同用此头。
    历史：曾走 auth1 Handshake（BasicAuth 载荷 → auth-token-bin 头）——arrow-java 的
    Handshake RPC 只经 auth1 ServerAuthHandler，与 JDBC 驱动所需的 auth2 互斥，
    网关已统一 auth2（见 FgFlightService）。"""

    def __init__(self):
        self.fc = fl.FlightClient(GW)
        self.token = self.fc.authenticate_basic_token(USER, PASSWORD)
        assert self.token[0] == b"authorization" and self.token[1].startswith(b"Bearer "), \
            f"unexpected token header: {self.token!r}"
        self.headers = [self.token]
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

def poll_flight_info(c, sql, expect_rows, max_polls=90,
                     min_pending=0, check_pending_schema=False, fetch_data=True):
    """pyarrow FlightClient 无 poll API → grpcio 直打 FlightService/PollFlightInfo。

    min_pending>0 断言至少经历 N 次 not-done 响应（pending 路径回归——短查询在首个
    poll 的 max-wait 内终态时永远踩不到该分支）；check_pending_schema 断言 pending
    响应的 FlightInfo 宣告非空 schema（行带 schema 出生：register 同步 AnalyzePlan）；
    fetch_data=False 跳过 DoGet 取数、以 FlightInfo.total_records 断言行数（重物化
    轻回流的 pending 用例用）。"""
    import grpc

    ch = grpc.insecure_channel("localhost:32010")
    # auth2：复用 Client 的 Authorization Basic 头
    md = ((c.headers[0][0].decode(), c.headers[0][1].decode()),)

    # 2) PollFlightInfo：请求体直接是 FlightDescriptor{descriptor_type=1:CMD(2), command=2:Any}
    any_msg = f_string(1, TYPE_QUERY) + f_bytes(2, f_string(1, sql))
    descriptor = (
        varint((1 << 3) | 0) + varint(2)  # descriptor_type = CMD = 2
        + f_bytes(2, any_msg))
    poll = ch.unary_unary("/arrow.flight.protocol.FlightService/PollFlightInfo",
                          request_serializer=lambda x: x, response_deserializer=lambda x: x)
    info_bytes = None
    pending = 0
    for i in range(max_polls):
        raw = poll(descriptor, metadata=md)
        fields = {f: v for f, w, v in walk(raw) if w == 2}
        fields_all = [f for f, w, v in walk(raw)]
        if 2 not in fields_all:  # flight_descriptor unset → 终态
            info_bytes = fields[1]
            print(f"[poll] terminal after {i + 1} polls (pending x{pending})")
            break
        pending += 1
        if check_pending_schema:
            # FlightInfo{schema=2}：pending 也必须宣告真实 schema（空 schema 会被严格
            # 客户端拒收，且曾因 not-done 分支取错行而 NPE/为空）
            schema_b = next(
                (v2 for f2, w2, v2 in walk(fields[1]) if f2 == 2 and w2 == 2), b"")
            assert len(schema_b) > 0, f"pending#{pending} advertised empty schema"
        print(f"[poll {i}] pending")
        time.sleep(1)
    assert info_bytes, "not terminal in time"
    assert pending >= min_pending, (
        f"pending x{pending} < {min_pending}：查询未跨过 max-wait，pending 路径未被覆盖")

    endpoints, records, qid = [], None, "?"
    for fno, w, v in walk(info_bytes):
        if fno == 3 and w == 2:
            endpoints.append(v)
        elif fno == 4 and w == 0:
            records = v
        elif fno == 7 and w == 2:
            qid = v.decode()
    print(f"[poll] endpoints={len(endpoints)} totalRecords={records} queryId={qid[:8]}...")
    if not fetch_data:
        assert records == expect_rows, f"totalRecords {records} != {expect_rows}"
        print("[poll] PASS（未取数，以 total_records 断言）")
        return qid

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
    md = ((c.headers[0][0].decode(), c.headers[0][1].decode()),)
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


def part_retry(c):
    """单分片可重试（F3/v0.16）：PART 票 DoGet 中途 kill → 同票重发全量。"""
    import time
    endpoints, records, qid = poll_terminal(c, "SELECT id FROM range(100000)")
    assert len(endpoints) == 2
    tickets = []
    for epb in endpoints:
        t, locs = parse_endpoint(epb)
        assert t is not None and all(not u.startswith("http") for u in locs)
        tickets.append(t)

    # part0：读首个 chunk 即断流
    r0 = c.do_get(fl.Ticket(tickets[0]))
    chunk0, _ = r0.read_chunk()
    print(f"[retry] part0 partial {chunk0.num_rows} rows, killing stream")
    close = getattr(r0, "close", None)
    if close:
        close()
    time.sleep(1)

    t0 = c.do_get(fl.Ticket(tickets[0])).read_all()
    t1 = c.do_get(fl.Ticket(tickets[1])).read_all()
    assert t0.num_rows == 50000 and t1.num_rows == 50000
    ids = sorted(t0.column("id").to_pylist() + t1.column("id").to_pylist())
    assert ids == list(range(100000))
    print("[retry] ALL PASS")
    return qid

def backpressure_timeout(c):
    """慢消费背压（Dremio putNextWhenClientReady 语义）：网关须以
    -Dfg.relay.client.readiness.timeout=2s 启动。part(~64MB) > gRPC 流控/BDP 窗口(~16MB)
    → 4s/chunk 慢消费下服务端 isReady 阻塞、2s 超时 fail，流提前中止而非静态缓冲吞掉。
    全程约 9 分钟（客户端需先排空 ~16MB 在途窗口才进入稳态阻塞）。"""
    t0 = time.time()
    endpoints, records, qid = poll_terminal(c, "SELECT id FROM range(8000000)")
    print(f"[bp] endpoints={len(endpoints)} totalRecords={records}")
    ticket, _ = parse_endpoint(endpoints[0])
    assert ticket is not None, "relay PART ticket expected"
    reader = c.do_get(fl.Ticket(ticket))
    rows, err = 0, None
    try:
        for chunk in reader:
            data = getattr(chunk, "data", None) or chunk
            rows += data.num_rows
            time.sleep(4.0)
    except Exception as e:
        err = e
        print(f"[bp] stream error after {rows} rows, t={time.time()-t0:.0f}s: {e}")
    assert err is not None, f"expected backpressure timeout error, got clean EOF rows={rows}"
    msg = str(err)
    assert "not ready" in msg or "backpressure" in msg or "Relay failed" in msg, f"unexpected error: {msg}"
    assert rows < 8_000_000, "stream must abort early, not deliver everything"
    print(f"[bp] aborted at rows={rows} (~{rows * 8 // 1024 // 1024}MB in flight drained): PASS")
    return qid


def legacy_long_wait(c):
    """STREAM 票 DoGet 等待预算与 fg.poll.max-wait 解耦（= fg.query.timeout）：
    网关须以 -Dfg.poll.max-wait=2s 启动——旧实现（DoGet 同 poll 预算）在查询 >2s 时
    DoGet 报 UNAVAILABLE；新实现 STREAM 票挂满查询全程直至终态。"""
    t0 = time.time()
    legacy_get_flight_info(
        c,
        "SELECT id % 1000 AS k, count(*) AS cnt FROM range(50000000) GROUP BY id % 1000",
        1000)
    print(f"[legacy-long] DoGet held {time.time() - t0:.1f}s under poll.max-wait=2s: PASS")


def adbc_query(c, sql, expect_rows, tag="adbc", extra_db_kwargs=None):
    """ADBC（adbc_driver_flightsql 1.x，C++ 驱动，dbapi）——⑥ 矩阵第四路客户端。
    支持度判据在服务端：gateway.log 有 Relay complete = relay 路径（DoGet）；https 模式下
    无 Relay complete 而行数正确 = PollFlightInfo + presigned HTTP GET；mode 落行见
    fg_operation.mode。extra_db_kwargs 注入连接选项（D15 头协商等）。"""
    import adbc_driver_flightsql.dbapi as dbapi
    t0 = time.time()
    # 127.0.0.1 而非 localhost：ADBC 内置 gRPC 的 macOS name resolver 对 localhost
    # 走 IPv6(::1) 探测，而网关 0.0.0.0 仅 IPv4 → 每次连接多耗 ~20s（pyarrow/grpcio 无此问题）
    kwargs = {"username": USER, "password": PASSWORD}
    kwargs.update(extra_db_kwargs or {})
    with dbapi.connect(uri="grpc://127.0.0.1:32010", db_kwargs=kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            table = cur.fetch_arrow_table()
    assert table.num_rows == expect_rows, f"rows {table.num_rows} != {expect_rows}"
    print(f"[{tag}] rows={table.num_rows} cols={table.column_names} ({time.time() - t0:.1f}s): PASS")


def syntax_error(c):
    """fail-fast 注册（行带 schema 出生）：语法错误在首个执行 RPC 即失败（引擎
    PARSE_SYNTAX_ERROR 透出）且不落 fg_operation 行（行数判据外部：psql count=0）——
    "宁可注册失败，不空宣告/不留半状态行"。"""
    t0 = time.time()
    try:
        legacy_get_flight_info(c, "SELEC broken FROM range(10)", 0)
        raise AssertionError("语法错误未 fail-fast（执行链路意外成功）")
    except Exception as e:
        assert "PARSE_SYNTAX_ERROR" in str(e), f"意外错误面: {type(e).__name__}: {e}"
    print(f"[syntax-error] PASS ({time.time() - t0:.1f}s，错误即刻浮出)")


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "legacy"
    c = client()
    if which == "legacy":
        legacy_get_flight_info(c, "SELECT id, id * 2 AS dbl, concat('v-', cast(id as STRING)) AS s FROM range(1000)", 1000)
    elif which == "legacy-long":
        legacy_long_wait(c)
    elif which == "poll":
        poll_flight_info(c, "SELECT id FROM range(1000)", 1000)
    elif which == "poll-pending":
        # pending（not-done）路径回归：与 cancel-poll 同款 -Dfg.poll.max-wait=2s 网关 +
        # 物化 >2s 的查询。恒慢的只有"物理物化"（sink 写不可被优化器折叠；count(*) 类
        # 会被代数折叠、聚合/纯计算随引擎热度从 2s 掉到 0.5s）→ 200M 物化 ~4s，且不取数
        # （fetch_data=False，以 total_records 断言，避免 relay 全量回流）。SQL 与 cancel
        # 家族区分开（指纹幂等）。断言 ≥1 次 pending 且 pending 响应宣告非空 schema。
        # 2026-09-25 教训：现有短查询用例全部在首个 poll 内终态，not-done 分支 NPE 逃逸整天
        poll_flight_info(
            c,
            "SELECT a.id FROM range(200000000) a JOIN range(100) b ON a.id % 100 = b.id",
            200000000, min_pending=1, check_pending_schema=True,
            fetch_data=False, max_polls=30)
    elif which == "syntax-error":
        syntax_error(c)
    elif which == "poll-multi":
        poll_flight_info(c, "SELECT id FROM range(5000) ORDER BY id", 5000)
    elif which == "https":
        https_presign(c, "SELECT id, id * 3 AS t FROM range(3000)", 3000, do_renew=True)
    elif which == "https-order":
        https_order_by(c)
    elif which == "retry":
        part_retry(c)
    elif which == "backpressure":
        backpressure_timeout(c)
    elif which == "adbc":
        adbc_query(c, "SELECT id, id * 2 AS dbl FROM range(1000)", 1000)
    elif which == "adbc-mode":
        # D15：https 模式经 ADBC 连接选项注入 RPC 头（adbc.flight.sql.rpc.call_header.<name>）。
        # SQL 与 adbc 案例区分：fingerprint 幂等（user+sql）下同 SQL 复用在册行（mode 首注册落行）
        adbc_query(c, "SELECT id, id * 4 AS quad FROM range(1000)", 1000, tag="adbc-mode",
                   extra_db_kwargs={"adbc.flight.sql.rpc.call_header.x-fg-endpoint-mode": "https"})
    elif which == "cancel":
        # ⚠ cancel 家族（py cancel / java cancel / jdbc cancel 与 cancel-poll）共用
        # 400M join 指纹（同 fg 用户）——用例间必须 TRUNCATE fg_operation，否则命中
        # 上一例的终态行（如 CANCELLED）得到假结果；另勿并行跑（local[2] 引擎会饱和）
        cancel_inflight(c, "SELECT a.id FROM range(400000000) a JOIN range(500) b ON a.id % 500 = b.id")
    else:
        raise SystemExit(f"unknown case {which}")
