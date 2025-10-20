import re
import os
from typing import Dict, Tuple

# 宽松且不区分大小写的正则：兼容 Write/Read, Table, Entry/entry, Md5/MD5 等变体
WRITE_LOG_PATTERN = re.compile(r"\bWrite\s+Table\s+(\d+)\s+Entry\s+(\d+)\s+Md5\s+([0-9a-fA-F]+)\b", re.IGNORECASE)
READ_LOG_PATTERN = re.compile(r"\bRead\s+Table\s+(\d+)\s+Entry\s+(\d+)\s+Md5\s+([0-9a-fA-F]+)\b", re.IGNORECASE)

# 时间戳（如果行中有），例如：2025-10-18 12:34:56 或带毫秒
TIMESTAMP_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)")

# identity 的匹配，忽略大小写
IDENTITY_PATTERN = re.compile(r"\bidentity\s+(\d+)\b", re.IGNORECASE)


def analyze_log(log_filepath: str):
    """
    分析日志文件，检查写入和读取的 Table Entry 及其 MD5 值，
    并检测同一 tableId 的 identity 是否一致。
    """
    print(f"--- 正在分析日志文件: {log_filepath} ---")

    # 存储写入记录：键 TableID-EntryID -> (最新写入md5, 写入次数, 第一次写入行号)
    written_entries: Dict[str, Tuple[str, int, int]] = {}

    # 存储读写 MD5 不一致的记录
    md5_mismatches: Dict[str, Dict] = {}

    # md5 -> 事件列表（用于反向映射）
    md5_map: Dict[str, list] = {}

    # tableId -> {'identities': set(...), 'occurrences': [ {identity,type,entry,line,time} ]}
    table_identities: Dict[str, Dict] = {}

    write_warning_count = 0
    read_mismatch_count = 0

    try:
        with open(log_filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                # 先尝试 Write
                write_match = WRITE_LOG_PATTERN.search(line)
                if write_match:
                    table_id = write_match.group(1)
                    entry_id = write_match.group(2)
                    md5_value = write_match.group(3).lower()
                    entry_key = f"{table_id}-{entry_id}"
                    ts_match = TIMESTAMP_PATTERN.search(line)
                    time_str = ts_match.group(1) if ts_match else None

                    id_match = IDENTITY_PATTERN.search(line)
                    identity = id_match.group(1) if id_match else None

                    if identity:
                        rec = table_identities.setdefault(table_id, {"identities": set(), "occurrences": []})
                        if rec["identities"] and identity not in rec["identities"]:
                            print(f"[IDENTITY冲突] 第 {line_num} 行: Table {table_id} 发现不同的 identity: {sorted(list(rec['identities']))} -> 新的 {identity}")
                        rec["identities"].add(identity)
                        rec["occurrences"].append({"identity": identity, "type": "write", "entry": entry_id, "line": line_num, "time": time_str})

                    md5_map.setdefault(md5_value, []).append({
                        "type": "write",
                        "table": table_id,
                        "entry": entry_id,
                        "line": line_num,
                        "time": time_str,
                    })

                    if entry_key in written_entries:
                        write_warning_count += 1
                        original_md5, count, first_line = written_entries[entry_key]
                        written_entries[entry_key] = (md5_value, count + 1, first_line)
                        print(f"[重复写入警告] 第 {line_num} 行: Table {table_id} Entry {entry_id}.")
                        if original_md5 != md5_value:
                            print(f"        > MD5 值变更! 第一次写入({first_line}行) MD5: {original_md5}, 本次写入 MD5: {md5_value}")
                        else:
                            print(f"        > MD5 值一致 ({original_md5})，但发生了重复写入。")
                    else:
                        written_entries[entry_key] = (md5_value, 1, line_num)
                    continue

                # 再尝试 Read
                read_match = READ_LOG_PATTERN.search(line)
                if read_match:
                    table_id = read_match.group(1)
                    entry_id = read_match.group(2)
                    read_md5 = read_match.group(3).lower()
                    entry_key = f"{table_id}-{entry_id}"
                    ts_match = TIMESTAMP_PATTERN.search(line)
                    time_str = ts_match.group(1) if ts_match else None

                    md5_map.setdefault(read_md5, []).append({
                        "type": "read",
                        "table": table_id,
                        "entry": entry_id,
                        "line": line_num,
                        "time": time_str,
                    })

                    id_match = IDENTITY_PATTERN.search(line)
                    identity = id_match.group(1) if id_match else None
                    if identity:
                        rec = table_identities.setdefault(table_id, {"identities": set(), "occurrences": []})
                        if rec["identities"] and identity not in rec["identities"]:
                            print(f"[IDENTITY冲突] 第 {line_num} 行: Table {table_id} 发现不同的 identity: {sorted(list(rec['identities']))} -> 新的 {identity}")
                        rec["identities"].add(identity)
                        rec["occurrences"].append({"identity": identity, "type": "read", "entry": entry_id, "line": line_num, "time": time_str})

                    if entry_key in written_entries:
                        write_md5, _, first_line = written_entries[entry_key]
                        if write_md5 != read_md5:
                            read_mismatch_count += 1
                            if entry_key not in md5_mismatches:
                                md5_mismatches[entry_key] = {
                                    "write_md5": write_md5,
                                    "first_write_line": first_line,
                                    "mismatches": []
                                }
                            md5_mismatches[entry_key]["mismatches"].append({
                                "line_num": line_num,
                                "read_md5": read_md5
                            })
                            print(f"[MD5冲突警告] 第 {line_num} 行发现读写 MD5 不一致: Table {table_id} Entry {entry_id}.")
                            print(f"        > 写入(最新): {write_md5}")
                            print(f"        > 读取(当前): {read_md5}")
                    continue

    except FileNotFoundError:
        print(f"错误: 找不到文件 {log_filepath}")
        return

    # 打印最终统计结果
    print("\n--- 最终分析结果 ---")
    if write_warning_count == 0:
        print("✅ 未发现重复写入警告。")
    else:
        print(f"⚠️ 发现 {write_warning_count} 次重复写入警告。")

    if read_mismatch_count == 0:
        print("✅ 未发现读写 MD5 不一致冲突。")
    else:
        print(f"❌ 发现 {read_mismatch_count} 次读写 MD5 不一致冲突，涉及 {len(md5_mismatches)} 个条目。")

    # 读写 MD5 不一致的条目详情（若无则说明，没有冲突，但仍继续后续检查）
    print("\n--- 读写 MD5 不一致的条目详情 ---")
    mismatch_list = []
    for key, data in md5_mismatches.items():
        mismatch_list.append((key, data))

    if not mismatch_list:
        print("✅ 无需列出具体条目：未发现读写 MD5 不一致的冲突。")
    else:
        mismatch_list.sort(key=lambda x: [int(p) for p in x[0].split('-')])
        for key, data in mismatch_list:
            print(f"\n[冲突条目] {key}: 首次写入于第 {data['first_write_line']} 行。")
            print(f"  > 最新写入 MD5: {data['write_md5']}")
            print(f"  > 共 {len(data['mismatches'])} 次读取 MD5 不一致：")
            for m in data['mismatches']:
                print(f"    - 第 {m['line_num']} 行读取 MD5: {m['read_md5']}")

    # Identity 一致性检查（始终执行）
    print("\n--- Identity 一致性检查 (按 TableId) ---")
    if not table_identities:
        print("ℹ️ 未在日志中发现任何 identity 信息。")
    else:
        inconsistent_tables = []
        for tbl, rec in sorted(table_identities.items(), key=lambda x: int(x[0])):
            ids = rec.get("identities", set())
            if len(ids) == 1:
                only_id = next(iter(ids))
                print(f"✅ Table {tbl}: identity 一致 ({only_id})，共 {len(rec.get('occurrences', []))} 次记录。")
            else:
                inconsistent_tables.append((tbl, ids, rec.get('occurrences', [])))
                print(f"❌ Table {tbl}: 存在多个 identity: {sorted(list(ids))}")
                for occ in rec.get('occurrences', []):
                    time_display = occ.get('time') or 'unknown'
                    print(f"    - {occ.get('type')} entry {occ.get('entry')} line {occ.get('line')} identity {occ.get('identity')} time {time_display}")

        if not inconsistent_tables:
            print("\n✅ 所有含 identity 的 tableId 的 identity 都一致。")

    # MD5 -> Entry 反向映射（仅列出映射到多个不同的条目的 MD5）
    print("\n--- MD5 -> Entry 反向映射（仅列出映射到多个不同的条目的 MD5） ---")
    multi_md5s = []
    for md5, events in md5_map.items():
        entries = set(f"{e['table']}-{e['entry']}" for e in events)
        if len(entries) > 1:
            multi_md5s.append((md5, entries, events))

    if not multi_md5s:
        print("✅ 未发现任何 MD5 映射到多个不同的条目。")
        return

    multi_md5s.sort(key=lambda x: x[0])
    for md5, entries, events in multi_md5s:
        print(f"\nMD5: {md5} 映射到 {len(entries)} 个不同的条目:")
        sorted_entries = sorted(list(entries), key=lambda k: [int(p) for p in k.split('-')])
        for ent in sorted_entries:
            t, e = ent.split('-')
            print(f"  - Entry {ent}:")
            writes = [ev for ev in events if ev['type'] == 'write' and ev['table'] == t and ev['entry'] == e]
            reads = [ev for ev in events if ev['type'] == 'read' and ev['table'] == t and ev['entry'] == e]
            if writes:
                for w in writes:
                    time_display = w['time'] if w['time'] else 'unknown'
                    print(f"      write @ line {w['line']}, time: {time_display}")
            else:
                print("      write: (无写入记录)")
            if reads:
                for r in reads:
                    time_display = r['time'] if r['time'] else 'unknown'
                    print(f"      read  @ line {r['line']}, time: {time_display}")
            else:
                print("      read : (无读取记录)")

# 主执行块
if __name__ == "__main__":
    DEFAULT_LOG_FILE = "entry.log"
    if not os.path.exists(DEFAULT_LOG_FILE):
        print(f"警告: 默认日志文件 '{DEFAULT_LOG_FILE}' 不存在。")
    else:
        analyze_log(DEFAULT_LOG_FILE)