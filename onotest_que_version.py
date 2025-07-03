import joblib
import time
import glob
import ssl
import json
import os
import logging
from datetime import datetime, timezone, timedelta
from collections import deque
import pandas as pd
import numpy as np 
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

# ロガーの設定
logging.basicConfig(level=logging.ERROR, filename='error.log', filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# .envファイルを読み込む
load_dotenv('./.env')

# ベースディレクトリの設定
BASE_DIR = '/home/js-027/Downloads/dgw_sdcard/dxpgateway/document/AJT-script'

# AWS IoT Coreの設定を環境変数から取得（エラーハンドリング追加）
def get_env_variable(var_name, default=None):
    """環境変数を安全に取得する関数"""
    value = os.getenv(var_name, default)
    if value is None:
        logger.error(f"環境変数 {var_name} が設定されていません")
        raise ValueError(f"環境変数 {var_name} が設定されていません")
    return value

try:
    AWS_IOT_ENDPOINT = get_env_variable("AWS_IOT_ENDPOINT")
    PATH_TO_CERT = get_env_variable("PATH_TO_CERT")
    PATH_TO_KEY = get_env_variable("PATH_TO_KEY")
    PATH_TO_ROOT = get_env_variable("PATH_TO_ROOT")
    TOPIC = get_env_variable("TOPIC")

    # ファイルの存在確認
    for file_path, name in [(PATH_TO_CERT, "証明書"), (PATH_TO_KEY, "秘密鍵"), (PATH_TO_ROOT, "ルート証明書")]:
        if not os.path.exists(file_path):
            logger.error(f"{name}ファイルが見つかりません: {file_path}")
            raise FileNotFoundError(f"{name}ファイルが見つかりません: {file_path}")

    logger.info("AWS IoT Core設定の読み込みが完了しました")

except (ValueError, FileNotFoundError) as e:
    logger.error(f"設定エラー: {e}")
    exit(1)

# 接続エラー時のコールバック関数を先に定義
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("AWS IoT Coreに正常に接続しました")
    else:
        logger.error(f"接続失敗: {rc}")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.error("Connection lost! Reconnecting...")
        while True:
            try:
                client.reconnect()
                logger.info("Reconnected!")
                break
            except Exception as e:
                logger.error(f"Reconnect failed: {e}, retrying in 5 seconds...")
                time.sleep(5)

# MQTTクライアントの設定（エラーハンドリング追加）
try:
    client = mqtt.Client()
    client.tls_set(PATH_TO_ROOT, certfile=PATH_TO_CERT, keyfile=PATH_TO_KEY)

    # コールバック関数を設定
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    # 接続の設定（タイムアウトとキープアライブ追加）
    client.connect(AWS_IOT_ENDPOINT, 8883, keepalive=60)
    # メインループ
    client.loop_start()
    logger.info("MQTTクライアントの初期化が完了しました")

except Exception as e:
    logger.error(f"MQTT接続エラー: {e}")
    exit(1)

# モデルを一度だけ読み込む（グローバル変数として）
try:
    LOADED_MODEL = joblib.load(f'{BASE_DIR}/random_forest_regressor.joblib')
    logger.info("機械学習モデルの読み込みが完了しました")
except Exception as e:
    logger.error(f"モデルファイルの読み込みエラー: {e}")
    exit(1)

    
def predict_value(DM00086_data, DM00126_data, DM00015_data, DM00037_data, DM00318_data, DM00027_data, DM00410_data, DM00420_data, MR103_data, MR010_data):
    
    # 現在の時間を取得
    now = datetime.now(timezone(timedelta(hours=9)))

    # 指定された形式でフォーマット
    formatted_time = now.strftime('%Y-%m-%dT%H:%M:%S%z')

    # タイムゾーンのコロンを追加
    formatted_time = formatted_time[:-2] + ':' + formatted_time[-2:]
    
    return {
        "tag": "pub_tag",
        "pub_seq": 0,
        "pub_member": {
            "entryDatetime": formatted_time,
            "mac_address": "10:cc:1b:18:04:5e",
            "device_type": "PLC1",
            "event_type": "1",
            "ConnectError": 0,
            "DM00086": int(DM00086_data) if DM00086_data is not None else 0,
            "DM00126": int(DM00126_data) if DM00126_data is not None else 0,
            "DM00015": int(DM00015_data) if DM00015_data is not None else 0,
            "DM00037": int(DM00037_data) if DM00037_data is not None else 0,
            "DM00318": int(DM00318_data) if DM00318_data is not None else 0,
            "DM00027": int(DM00027_data) if DM00027_data is not None else 0,
            "DM00410": int(DM00410_data) if DM00410_data is not None else 0,
            "DM00420": float(DM00420_data) if DM00420_data is not None else 0,
            "MR103": int(MR103_data) if MR103_data is not None else 0,
            "MR010": int(MR010_data) if MR010_data is not None else 0,
        }
    }

def csv_file_creation(Turbidity_raw, pH_raw):
    """
    variable:
    DM00086: Turbidity_raw
    DM00126: pH_raw
    
    image
    JT_PAC, Turbidity_raw, pH_raw
    5, 7.16, 7.48, 0.1
    ~
    50, 7.16, 7.48, 0.1
    """

    data = {
        "JT_PAC": list(range(5, 51)),
        "Turbidity_raw": [Turbidity_raw] * 46,
        "pH_raw": [pH_raw] * 46,
    }

    df = pd.DataFrame(data)
    df.to_csv(f'{BASE_DIR}/setting_data.csv', index=False)

def Data_analysis(data, Target_value):
    """
    variable:
    DM00027: Target value
    """

    # 特徴量とターゲット変数の定義（再度定義する必要がある場合）
    X = data[['JT_PAC', 'Turbidity_raw', 'pH_raw']]
    try:
        y = data['JT_turbidity_LN']
    except:
        y = None

    # テストデータでの予測（グローバル変数のモデルを使用）
    y_pred = LOADED_MODEL.predict(X)

    # 予測値を元のスケールに戻す
    y_pred_original_scale = np.exp(y_pred)

    if y is not None:
        # モデルの性能評価（元のスケールで評価）
        mse = mean_squared_error(np.exp(y), y_pred_original_scale)
        logger.info(f'Mean Squared Error (original scale): {mse}')
    
    data['JT_turbidity_LN'] = y_pred_original_scale
    
    # 0.1以下で最も大きいJT_turbidity_LNの値を持つ行をフィルタリング
    filtered_data = data[data['JT_turbidity_LN'] <= Target_value]

    # JT_turbidity_LNの最大値を持つ行をフィルタリング
    max_turbidity_rows = filtered_data[filtered_data['JT_turbidity_LN'] == filtered_data['JT_turbidity_LN'].max()]

    # その中でJT_PACの値が最大の行を取得
    max_pac_row = max_turbidity_rows[max_turbidity_rows['JT_PAC'] == max_turbidity_rows['JT_PAC'].max()]
    
    # 結果の表示（必要に応じて）
    logger.info(f'max_pac_row: {max_pac_row}')

    # max_pac_rowが空でないかを確認
    if not max_pac_row.empty:
        jt_pac_value = max_pac_row["JT_PAC"].values[0]  # .valuesを使用して配列を取得し、最初の要素を選択
    else:
        jt_pac_value = None  # または適切なデフォルト値を設定

    # DataFrameをJT_turbidity_LNで昇順にソートし、その後JT_PACでも昇順にソート
    sorted_df = data.sort_values(by=['JT_turbidity_LN', 'JT_PAC'], ascending=[True, True])
    # ソートされたデータを表示
    logger.info(f'sorted_df: {sorted_df}')

    return jt_pac_value

def Data_analysis_DM000318(Injection_rate, Turbidity_raw, pH_raw):
    # 特徴量とターゲット変数の定義（再度定義する必要がある場合）
    X = [[Injection_rate, Turbidity_raw, pH_raw]]

    # テストデータでの予測（グローバル変数のモデルを使用）
    y_pred = LOADED_MODEL.predict(X)
    
    # 予測値を元のスケールに戻す
    y_pred_original_scale = np.exp(y_pred)
    
    return y_pred_original_scale
    
if __name__ == '__main__':
    # PLCトリガーを取得、実行
    logger.info("Starting the process...")

    # 起動時にCSVファイルのデータのみをクリアする処理を追加
    def clear_csv_data():
        csv_files = {
            'trigger': f'{BASE_DIR}/trigger.csv',
            'file_write_raw': f'{BASE_DIR}/file_write_raw.csv',
            'result_trigger': f'{BASE_DIR}/result_trigger.csv',
            '1537': f'{BASE_DIR}/1537.csv'
        }

        for file_name, file_path in csv_files.items():
            try:
                # CSVファイルを読み込む
                df = pd.read_csv(file_path)

                # データ部分のみを削除（ヘッダーは保持）
                df_cleared = df.iloc[0:0]

                # ヘッダーを保持したまま保存
                df_cleared.to_csv(file_path, index=False)
                logger.info(f"Cleared data from {file_name}.csv while keeping headers")
            except Exception as e:
                logger.error(f"Error clearing data from {file_name}.csv: {e}")
                logger.error(f"Error details: {str(e)}")

    # プログラム起動時にCSVファイルのデータをクリア
    logger.info("Starting the process... Clearing CSV data while keeping headers")
    clear_csv_data()
    
    def generate_signal():
        df = pd.read_csv(f'{BASE_DIR}/trigger.csv')

        # データ部分を削除し、ヘッダーのみを保持
        df_delete = df.iloc[0:0]

        # 変更を同じファイルに上書き保存
        df_delete.to_csv(f'{BASE_DIR}/trigger.csv', index=False)
        
        for index, row in df.iterrows():
            if row['MR010'] == 1:
                return 1
            else:
                # CSVファイルを読み込む
                df = pd.read_csv(f'{BASE_DIR}/file_write_raw.csv')
                
                # データ部分を削除し、ヘッダーのみを保持
                df_delete_2 = df.iloc[0:0]

                # 変更を同じファイルに上書き保存
                df_delete_2.to_csv(f'{BASE_DIR}/file_write_raw.csv', index=False)
              
  
    def flatten_nested_deque(queue):
        flattened_queue = deque()
        for item in queue:
            if isinstance(item, list):
                flattened_queue.extend(item)
            else:
                flattened_queue.append(item)
        flattened_queue = [item for item in flattened_queue if item is not None]
        logger.info(f"キュー内の信号: {flattened_queue}")
        return flattened_queue


    def process_signals():
   
        signal_queue = deque()

        # 前回の値を保持する変数を初期化
        previous_DM15 = None
        previous_DM37 = None

        while True:
            try:
                signal = generate_signal()
                
                # ランダムに信号を生成し、キューに追加
                logger.info(f"受信した信号: {signal}")
                signal_queue.append(signal)
                
                signal_queue = flatten_nested_deque(signal_queue)

                # キュー内の信号を確認し、1が入っているかチェック
                if all(signal in signal_queue for signal in [1]):
                    # 全ての信号が揃っていれば、各信号を1つずつ処理
                    for signal_to_process in [1]:
                        signal_queue.remove(signal_to_process)
                        logger.info(f"処理された信号: {signal_to_process}")
                        
                    # ここでMR10が1のときに実行するスクリプトを呼び出す
                    # CSVファイルを読み込む
                    df = pd.read_csv(f'{BASE_DIR}/file_write_raw.csv')
                    # データ部分を削除し、ヘッダーのみを保持
                    df_delete_2 = df.iloc[0:0]
                    # 変更を同じファイルに上書き保存
                    df_delete_2.to_csv(f'{BASE_DIR}/file_write_raw.csv', index=False)
                    
                    # MR010, 103
                    df_010 = pd.read_csv(f'{BASE_DIR}/result_trigger.csv')
                    df_delete_2_010 = df_010.iloc[0:0]
                    df_delete_2_010.to_csv(f'{BASE_DIR}/result_trigger.csv', index=False)

                    # DM15, 37
                    df_1537 = pd.read_csv(f'{BASE_DIR}/1537.csv')
                    df_delete_2_1537 = df_1537.iloc[0:0]
                    df_delete_2_1537.to_csv(f'{BASE_DIR}/1537.csv', index=False)

                    # dfが少なくとも1行のデータを持っているかどうかをチェック
                    if len(df) > 0:
                        # dfにDM00086, DM00126, DM00027の各カラムが存在し、それぞれの値がNaNでないことを確認
                        if all(pd.notna(df[col].values[0]) for col in ['DM00086', 'DM00126', 'DM00027', 'DM00318']):
                            Turbidity_raw = df['DM00086'].values[0] / 10
                            pH_raw = df['DM00126'].values[0] / 1000
                            csv_file_creation(Turbidity_raw, pH_raw)
                            
                            Target_value = df['DM00027'].values[0] / 100
                            Injection_rate = df['DM00318'].values[0] / 10

                            Analysis_data = pd.read_csv(f'{BASE_DIR}/setting_data.csv')
                            Best_JT_PAC = Data_analysis(Analysis_data, Target_value)
                            
                            Injection_rate_result = Data_analysis_DM000318(Injection_rate, Turbidity_raw, pH_raw)
                            logger.info(f'Injection_rate_result: {Injection_rate_result}')
                            
                            # Best_JT_PACを含むDataFrameを作成
                            df_result = pd.DataFrame([Best_JT_PAC], columns=['Best_JT_PAC'])

                            # DataFrameをCSVファイルに保存
                            df_result.to_csv(f'{BASE_DIR}/Best_JT_PAC.csv', header=False, index=False)
                            
                            # Injection_rate_resultをDataFrameに変換して保存
                            df_injection_rate_result = pd.DataFrame(Injection_rate_result)
                            
                            # 配列の要素を取り出して浮動小数点数に変換
                            injection_rate_float = float(df_injection_rate_result.values[0][0])
                            
                            # injection_resultを新しいCSVファイルに出力（常に上書き）
                            df_injection_result_output = pd.DataFrame([[injection_rate_float]], columns=['injection_result'])
                            df_injection_result_output.to_csv(f'{BASE_DIR}/injection_result.csv', mode='w', header=False, index=False)
                            
                            # df_injection_rate_result.to_csv(f'{BASE_DIR}/Best_JT_PAC_2.csv', header=False, index=False)
                            
                            # result_total_data.csvにBest_JT_PACを追加
                            # 新しいデータを追加
                            df_result['pH_raw'] = pH_raw
                            df_result['Turbidity_raw'] = Turbidity_raw
                            df_result['Target_value'] = Target_value
                            df_result['Time'] = [datetime.now().strftime('%Y-%m-%d %H:%M:%S') for _ in range(len(df_result))]
                            
                            # 列の順序を入れ替え
                            columns = list(df_result.columns)
                            best_jt_pac_index = columns.index('Best_JT_PAC')
                            time_index = columns.index('Time')
                            columns[best_jt_pac_index], columns[time_index] = columns[time_index], columns[best_jt_pac_index]
                            df_result = df_result[columns]
                            
                            # Injection_rate_resultをSeriesに変換
                            injection_rate_series = pd.Series(Injection_rate_result, name='Injection_rate_result')

                            # SeriesをDataFrameに変換してから結合
                            df_result = pd.concat([df_result, injection_rate_series], axis=1)
                            
                            MR010 = df_010['MR010'].values[0]
                            MR103 = df_010['MR103'].values[0]
                            
                            df_result['MR010'] = MR010
                            df_result['MR103'] = MR103
                            
                            # メッセージ送信のコールバック
                            def on_publish(client, userdata, mid):
                                logger.info("Message Published...")
                            
                            # df_1537にデータがあるか確認
                            if not df_1537.empty:
                                DM15 = df_1537['recive_15'].values[0]
                                DM37 = df_1537['recive_37'].values[0]
                                
                                df_result['DM15'] = DM15
                                df_result['DM37'] = DM37
                                
                                # 前回の値を更新
                                previous_DM15 = DM15
                                previous_DM37 = DM37
                                
                                # 予測値を取得
                                y_pred_original_scale = predict_value(df['DM00086'].values[0], df['DM00126'].values[0], df_result['DM15'].values[0], df_result['DM37'], df['DM00318'].values[0], df['DM00027'].values[0], Best_JT_PAC if Best_JT_PAC is not None else 0, injection_rate_float, df_010['MR010'].values[0], df_010['MR103'].values[0])
                                
                                client.on_publish = on_publish

                                # メッセージを送信
                                topic = TOPIC
                                
                                message_dict = y_pred_original_scale
                                
                                # 辞書をJSON文字列に変換
                                message = json.dumps(message_dict)
                                result = client.publish(topic, message)

                                # 結果を確認
                                status = result.rc
                                if status == 0:
                                    logger.info(f"Message sent to topic `{topic}`")
                                else:
                                    logger.info(f"Failed to send message to topic `{topic}`")
                            else:
                                # df_1537にデータがない場合は前回の値を使用
                                if previous_DM15 is not None and previous_DM37 is not None:
                                    df_result['DM15'] = previous_DM15
                                    df_result['DM37'] = previous_DM37
                                    
                                    # 予測値を取得
                                    y_pred_original_scale = predict_value(df['DM00086'].values[0], df['DM00126'].values[0], df_result['DM15'].values[0], df_result['DM37'], df['DM00318'].values[0], df['DM00027'].values[0], Best_JT_PAC if Best_JT_PAC is not None else 0, injection_rate_float, df_010['MR010'].values[0], df_010['MR103'].values[0])
                                    
                                    client.on_publish = on_publish

                                    # メッセージを送信
                                    topic = TOPIC
                                    
                                    message_dict = y_pred_original_scale
                                    
                                    # 辞書をJSON文字列に変換
                                    message = json.dumps(message_dict)
                                    result = client.publish(topic, message)

                                    # 結果を確認
                                    status = result.rc
                                    if status == 0:
                                        logger.info(f"Message sent to topic2 `{topic}`")
                                    else:
                                        logger.info(f"Failed to send message to topic2 `{topic}`")
                                
                            # df_resultをCSVファイルに保存
                            file_path = f'{BASE_DIR}/result_total_data.csv'
                            df_result.to_csv(file_path, mode='a', header=False, index=False)
                            
                            logger.info(f'y_pred_original_scale: {y_pred_original_scale}')
                                
                time.sleep(3)  # デモのために3秒待機
            except Exception as e:
                logger.error(f"An error occurred: {e}")
                # エラーが発生した場合に少し待機してから再開
                time.sleep(120)
    process_signals()

