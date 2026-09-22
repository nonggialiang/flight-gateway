#!/usr/bin/env python3
"""
FG M1 端到端主链路验证（design §8/§9）：pyarrow flight_sql_client。

前置（本地联调环境）：
  docker run -d --name fg-pg -e POSTGRES_DB=flightgateway -e POSTGRES_USER=fg -e POSTGRES_PASSWORD=fg -p 5432:5432 postgres:16
  docker run -d --name fg-minio -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address :9001
  docker exec fg-minio mc alias set local http://localhost:9000 minioadmin minioadmin && docker exec fg-minio mc mb local/fg-results
  # 引擎薄壳（本地）：
  SPARK_HOME=<spark-dist> $SPARK_HOME/bin/spark-submit --master "local[2]" \
    --class org.fg.spark.app.FgEngineMain \
    --jars fg-engine-spark/fg-spark-sink/target/fg-spark-sink-*.jar \
    fg-engine-spark/fg-spark-app/target/fg-spark-app-*.jar
  # gateway：
  java -Dfg.flight.port=32010 -Dfg.result.endpoint.mode=relay \
       -jar fg-dist/target/fg-dist-*.jar

用法：python3 e2e/run_e2e.py [--mode relay|https] [--sql "SELECT 1 AS id, 'x' AS v"]
"""
import argparse
import sys
import time

import pyarrow
import pyarrow.flight
import pyarrow.flight.sql

GW = "grpc://localhost:32010"
USER = "fg"
PASSWORD = "fg"


def poll_until_terminal(client, sql, max_polls=120):
    """首 poll=注册+触发+快返；后续=长等待至终态（PollInfo.flight_descriptor unset）。"""
    descriptor = pyarrow.flight.FlightDescriptor.for_command(
        pyarrow.flight.sql.get_command_statement_query(sql))
    info = None
    for i in range(max_polls):
        poll_info = client.poll_flight_info(descriptor)
        info = poll_info.info
        if poll_info.flight_descriptor is None:
            print(f"[poll] terminal after {i + 1} polls; "
                  f"endpoints={len(info.endpoints)} rows={info.total_records}")
            return info
        print(f"[poll {i}] not ready (endpoints={len(info.endpoints)}), retrying...")
        time.sleep(1)
    raise TimeoutError("query did not finish in time")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="relay", choices=["relay", "https"])
    parser.add_argument("--sql", default="SELECT 1 AS id, 'x' AS v")
    parser.add_argument("--legacy", action="store_true", help="GetFlightInfo 兼容路径")
    args = parser.parse_args()

    client = pyarrow.flight.sql.FlightSqlClient(GW, username=USER, password=PASSWORD)

    if args.legacy:
        info = client.execute(args.sql)
        print(f"[legacy GetFlightInfo] endpoints={len(info.endpoints)}")
    else:
        info = poll_until_terminal(client, args.sql)

    rows = 0
    for endpoint in info.endpoints:
        if args.mode == "https":
            import urllib.request
            url = endpoint.locations[0].get().decode() if hasattr(endpoint.locations[0], "get") \
                else str(endpoint.locations[0])
            with urllib.request.urlopen(url) as resp:
                reader = pyarrow.ipc.open_stream(resp.read())
                table = reader.read_all()
                rows += table.num_rows
                print(f"[https] part rows={table.num_rows}")
        else:
            reader = client.do_get(endpoint.ticket)
            table = reader.read_all()
            rows += table.num_rows
            print(f"[relay] part rows={table.num_rows} cols={table.column_names}")

    print(f"TOTAL_ROWS={rows}")
    if rows == 0:
        print("E2E_FAILED: no rows")
        return 1
    print("E2E_OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
