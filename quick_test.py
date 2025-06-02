#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MQTT通信量簡易テストスクリプト
現在のメッセージサイズや送信頻度を確認
"""

import json
import sys
import os
from datetime import datetime, timezone, timedelta

def test_message_size():
    """メッセージサイズをテスト"""
    print("=" * 50)
    print("MQTT メッセージサイズテスト")
    print("=" * 50)
    
    # 現在のメッセージ形式でサイズをテスト
    now = datetime.now(timezone(timedelta(hours=9)))
    formatted_time = now.strftime('%Y-%m-%dT%H:%M:%S%z')
    formatted_time = formatted_time[:-2] + ':' + formatted_time[-2:]
    
    # 標準的なメッセージ
    standard_message = {
        "tag": "pub_tag",
        "pub_seq": 0,
        "pub_member": {
            "entryDatetime": formatted_time,
            "mac_address": "10:cc:1b:18:04:5e",
            "device_type": "PLC1",
            "event_type": "1",
            "ConnectError": 0,
            "DM00086": 100,
            "DM00126": 7000,
            "DM00015": 150,
            "DM00037": 200,
            "DM00318": 250,
            "DM00027": 300,
            "DM00410": 400,
            "DM00420": 500.5,
            "MR103": 1,
            "MR010": 1,
        }
    }
    
    # JSONサイズを計算
    json_message = json.dumps(standard_message)
    json_size = len(json_message.encode('utf-8'))
    
    print(f"メッセージサイズ: {json_size} バイト")
    print(f"1分間に20回送信した場合: {json_size * 20} バイト/分")
    print(f"1時間に1200回送信した場合: {json_size * 1200 / 1024:.2f} KB/時")
    print(f"1日に28800回送信した場合: {json_size * 28800 / 1024 / 1024:.2f} MB/日")
    print()
    
    # データが大きくなる場合のテスト
    large_message = standard_message.copy()
    large_message["pub_member"]["additional_data"] = "x" * 1000  # 1KB追加
    
    large_json = json.dumps(large_message)
    large_size = len(large_json.encode('utf-8'))
    
    print(f"大きなメッセージサイズ: {large_size} バイト")
    print(f"サイズ比較: {large_size / json_size:.2f}倍")
    print()
    
    print("メッセージ内容プレビュー:")
    print(json.dumps(standard_message, indent=2, ensure_ascii=False)[:300] + "...")
    
    return json_size, large_size

def analyze_current_logs():
    """現在のログファイルを分析"""
    print("\n" + "=" * 50)
    print("現在のログファイル分析")
    print("=" * 50)
    
    # CSVファイルのサイズをチェック
    csv_files = [
        'result_total_data.csv',
        'setting_data.csv',
        'trigger.csv',
        'result_trigger.csv',
        'file_write_raw.csv',
        '1537.csv'
    ]
    
    total_size = 0
    for csv_file in csv_files:
        if os.path.exists(csv_file):
            size = os.path.getsize(csv_file)
            total_size += size
            print(f"{csv_file}: {size:,} バイト")
        else:
            print(f"{csv_file}: ファイルが見つかりません")
    
    print(f"\nCSVファイル合計サイズ: {total_size:,} バイト ({total_size/1024:.2f} KB)")
    
    # nohup.outのサイズ
    if os.path.exists('nohup.out'):
        nohup_size = os.path.getsize('nohup.out')
        print(f"nohup.out: {nohup_size:,} バイト ({nohup_size/1024/1024:.2f} MB)")
    
    return total_size

def calculate_traffic_estimates():
    """通信量の推定値を計算"""
    print("\n" + "=" * 50)
    print("通信量推定計算")
    print("=" * 50)
    
    # 現在の設定での推定
    message_size = 400  # 概算バイト数
    
    scenarios = [
        ("現在設定（3秒間隔）", 3, 20),
        ("1秒間隔の場合", 1, 60),
        ("30秒間隔の場合", 30, 2),
        ("エラー時の場合（1秒間隔×2重送信）", 1, 120)
    ]
    
    for scenario_name, interval, messages_per_minute in scenarios:
        daily_messages = messages_per_minute * 60 * 24
        daily_bytes = daily_messages * message_size
        
        print(f"\n{scenario_name}:")
        print(f"  送信間隔: {interval}秒")
        print(f"  1分あたり: {messages_per_minute} メッセージ")
        print(f"  1日あたり: {daily_messages:,} メッセージ")
        print(f"  1日データ量: {daily_bytes/1024/1024:.2f} MB")
        
        if scenario_name == "現在設定（3秒間隔）":
            baseline = daily_bytes
        else:
            ratio = daily_bytes / baseline if 'baseline' in locals() else 1
            print(f"  基準比: {ratio:.1f}倍")

def main():
    print("MQTT通信量分析ツール")
    print("実行日時:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    # メッセージサイズテスト
    standard_size, large_size = test_message_size()
    
    # ログ分析
    analyze_current_logs()
    
    # 通信量推定
    calculate_traffic_estimates()
    
    print("\n" + "=" * 50)
    print("�� 調査のための推奨事項:")
    print("=" * 50)
    print("1. 現在の送信間隔が本当に3秒かどうか確認")
    print("2. 複数の送信処理が重複していないか確認")
    print("3. エラー時の再送処理をチェック")
    print("4. メッセージ内容に不要なデータが含まれていないか確認")
    print("5. ログファイルのサイズ増加をチェック")
    
    print("\n📊 リアルタイム監視を開始するには:")
    print("python3 realtime_monitor.py")

if __name__ == "__main__":
    main() 
