1. Total Inffered flows:
from(bucket: "nids_bucket")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "network_flow")
  |> filter(fn: (r) => r._field == "predicted_label")
  |> group() 
  |> count()
  |> yield(name: "total_inferred_flows")

2. Top Threat Clusters:
import "strings"

from(bucket: "nids_bucket")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "network_flow")
  |> filter(fn: (r) => r._field == "predicted_label")
  |> filter(fn: (r) => r._value > 0)
  |> filter(fn: (r) => r.src_ip != "[]")
  
  |> map(fn: (r) => ({ r with 
      clean_ip: strings.replaceAll(v: strings.replaceAll(v: strings.replaceAll(v: r.src_ip, t: "[", u: ""), t: "]", u: ""), t: "'", u: "")
  }))
  
  |> map(fn: (r) => ({ r with 
      attack_type: 
        if r._value == 1 then "Bruteforce"
        else if r._value == 2 then "DDoS"
        else if r._value == 3 then "DoS"
        else if r._value == 4 then "Malware"
        else if r._value == 5 then "MITM"
        else if r._value == 6 then "Recon"
        else if r._value == 7 then "Web"
        else "Unknown"
  }))
  
  |> group(columns: ["clean_ip", "attack_type"])
  |> count(column: "_value")
  |> group() 
  |> sort(columns: ["_value"], desc: true)
  |> limit(n: 15)
  |> drop(columns: ["_start", "_stop", "_measurement", "_field", "src_ip"])
  |> yield(name: "top_attack_clusters")


3. Attack Distribution:
from(bucket: "nids_bucket")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "network_flow")
  |> filter(fn: (r) => r._field == "predicted_label")
  |> filter(fn: (r) => r._value > 0)
  
  |> map(fn: (r) => ({ r with 
      attack_type: 
        if r._value == 1 then "Bruteforce"
        else if r._value == 2 then "DDoS"
        else if r._value == 3 then "DoS"
        else if r._value == 4 then "Malware"
        else if r._value == 5 then "MITM"
        else if r._value == 6 then "Recon"
        else if r._value == 7 then "Web"
        else "Unknown"
  }))
  
  |> group(columns: ["attack_type"])
  |> count(column: "_value")
  |> group()
  |> yield(name: "attack_distribution")

4. Benign vs Malicious Flows:
from(bucket: "nids_bucket")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "network_flow")
  |> filter(fn: (r) => r._field == "predicted_label")
  |> map(fn: (r) => ({
      r with
      status: if r._value == 0 then "Benign" else "Malicious"
  }))
  |> group(columns: ["status"])
  |> count(column: "_value")
  |> keep(columns: ["status", "_value"])
  |> rename(columns: {_value: "count"})

5. Malicious Flow Trend by Time:
from(bucket: "nids_bucket")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "network_flow")
  |> filter(fn: (r) => r._field == "predicted_label")
  |> filter(fn: (r) => r._value > 0)
  
  |> map(fn: (r) => ({ r with 
      attack_name: 
        if r._value == 1 then "Bruteforce"
        else if r._value == 2 then "DDoS"
        else if r._value == 3 then "DoS"
        else if r._value == 4 then "Malware"
        else if r._value == 5 then "MITM"
        else if r._value == 6 then "Recon"
        else if r._value == 7 then "Web"
        else "Unknown"
  }))
  
  |> group(columns: ["attack_name"]) 
  |> aggregateWindow(every: v.windowPeriod, fn: count, createEmpty: true)
  |> yield(name: "attack_types")

6. Normalized Packet Size Trend:
from(bucket: "nids_bucket")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "network_flow")
  |> filter(fn: (r) => r._field == "network_packet-size_avg")
  |> group() 
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> yield(name: "avg_packet_size")

7. Normalized Average TTL Trend
from(bucket: "nids_bucket")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "network_flow")
  |> filter(fn: (r) => r._field == "network_ttl_avg")
  |> group() 
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: true)
  |> yield(name: "avg_ttl")

Lưu ý:
Min time range: 2025-01-15T13:04:54.353Z
Max time range: 2025-09-09T15:09:39.400Z

8. Code truy vấn cho influxdb:
from(bucket: "nids_bucket")
  |> range(start: 2025-01-15T13:04:54.353Z, stop: 2025-09-09T15:09:39.400Z)
  |> filter(fn: (r) => r._measurement == "network_flow")
  |> filter(fn: (r) => r._field == "predicted_label")
  |> map(fn: (r) => ({ r with count: 1 }))
  |> group(columns: ["_value"])
  |> sum(column: "count")
  |> rename(columns: {_value: "predicted_label"})


9. Xóa sạch pipeline:
docker compose down -v
rmdir /s /q C:\spark_checkpoints_cciot
docker compose up -d
python data_generator.py
py -3.11 spark_streaming.py
python daemon.py
