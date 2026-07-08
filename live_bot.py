import os
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

import numpy as np
import pandas as pd
import pandas_ta as ta
import ccxt
import tensorflow as tf
import xgboost as xgb
from supabase import create_client


# 1. Create a dummy handler to respond to Render's health checks
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bot is alive and kicking!")
    
    def log_message(self, format, *args):
        # Suppress spammy log messages from Render's health checks
        return

# 2. Function to spin up the server on the port Render assigns
def run_health_check_server():
    port = int(os.environ.get("PORT", 10000)) # Render automatically provides this env variable
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    print(f"📡 Dummy health check server listening on port {port}...")
    server.serve_forever()

# Start the health check server in a daemon thread
threading.Thread(target=run_health_check_server, daemon=True).start()

# 3. Initialize Exchange API (CCXT)
# Using public data tracking for BTC/USDT (or BTC/USD depending on exchange pairs)
exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {
        'defaultType': 'spot'
    }
})

# 4. Load Pre-trained Models
print("💾 Loading machine learning models...")
try:
    # Load LSTM Keras model
    lstm_model = tf.keras.models.load_model('lstm_feature_extractor.keras')
    
    # Load XGBoost model
    xgb_model = xgb.XGBClassifier()
    xgb_model.load_model('xgb_model.json')
    print("✅ Models loaded successfully!")
except Exception as e:
    print(f"❌ Error loading models: {e}")
    exit(1)

# 5. Define Tabular Columns matching your training dataset features
tabular_columns = [
    'atr', 'atr_pct', 'dist_vwap', 'chop', 'bb_width', 'is_squeeze',
    'rsi', 'rsi_slope', 'macd_hist', 'macd_hist_slope', 'ema_9', 'ema_21',
    'ema_50', 'trend_bull', 'trend_bear', 'body_size', 'upper_wick',
    'lower_wick', 'wick_ratio', 'rvol'
]

# 6. Initialize Supabase
supabase_url = 'https://bpizkikscieyhzajrrrg.supabase.co'
supabase_key = 'sb_publishable_UwkvMmq91Y5jS1PEJ9IE_w_vN5-JWUP'
supabase = create_client(supabase_url, supabase_key)

print("🟢 Starting Live 15m Inference Loop...")

lookback_limit = 100 

while True:
    try:
        # 1. Fetch the latest live data
        # Fetching BTC/USDT as a standard proxy for BTC/USD data on Binance
        ohlcv = exchange.fetch_ohlcv('BTC/USDT', '15m', limit=lookback_limit)
        live_df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # Get the timestamp of the recently closed candle
        latest_timestamp = live_df['timestamp'].iloc[-2] # -1 is the currently forming candle, -2 is closed
        current_price = live_df['close'].iloc[-2]
        
        # 2. Calculate Features exactly as you did in training
        live_df['atr'] = live_df.ta.atr(length=14)
        live_df['atr_pct'] = (live_df['atr'] / live_df['close']) * 100
        live_df['vwap_20'] = (live_df['volume'] * live_df['close']).rolling(window=20).sum() / live_df['volume'].rolling(window=20).sum()
        live_df['dist_vwap'] = (live_df['close'] - live_df['vwap_20']) / live_df['vwap_20'] * 100
        live_df['chop'] = live_df.ta.chop(length=14)
        
        bbands = live_df.ta.bbands(length=20, std=2)
        kc = live_df.ta.kc(length=20, scalar=1.5)
        live_df['bb_width'] = bbands[bbands.columns[0]]
        live_df['is_squeeze'] = ((bbands[bbands.columns[1]] > kc[kc.columns[0]]) & (bbands[bbands.columns[3]] < kc[kc.columns[2]])).astype(int)
        
        live_df['rsi'] = live_df.ta.rsi(length=14)
        live_df['rsi_slope'] = live_df['rsi'].diff(3) / 3
        
        macd = live_df.ta.macd(fast=12, slow=26, signal=9)
        live_df['macd_hist'] = macd['MACDh_12_26_9']
        live_df['macd_hist_slope'] = live_df['macd_hist'].diff(2)
        
        live_df['ema_9'] = live_df.ta.ema(length=9)
        live_df['ema_21'] = live_df.ta.ema(length=21)
        live_df['ema_50'] = live_df.ta.ema(length=50)
        live_df['trend_bull'] = ((live_df['ema_9'] > live_df['ema_21']) & (live_df['ema_21'] > live_df['ema_50'])).astype(int)
        live_df['trend_bear'] = ((live_df['ema_9'] < live_df['ema_21']) & (live_df['ema_21'] < live_df['ema_50'])).astype(int)
        
        live_df['body_size'] = abs(live_df['close'] - live_df['open'])
        live_df['upper_wick'] = live_df['high'] - live_df[['open', 'close']].max(axis=1)
        live_df['lower_wick'] = live_df[['open', 'close']].min(axis=1) - live_df['low']
        live_df['wick_ratio'] = (live_df['upper_wick'] - live_df['lower_wick']) / live_df['body_size'].replace(0, 0.0001)
        live_df['rvol'] = live_df['volume'] / live_df['volume'].rolling(window=20).mean()
        
        # Sequence features
        live_df['ret_close'] = live_df['close'].pct_change() * 100
        live_df['ret_high'] = ((live_df['high'] - live_df['close'].shift()) / live_df['close'].shift()) * 100
        live_df['ret_low'] = ((live_df['low'] - live_df['close'].shift()) / live_df['close'].shift()) * 100
        live_df['vol_pct'] = live_df['volume'].pct_change()

        # Clean and extract the last 10 candles for the sequence
        live_df.replace([np.inf, -np.inf], 0, inplace=True)
        live_df.dropna(inplace=True)
        
        # Extract the sequence and tabular data for the most recent closed candle
        seq_data = live_df[['ret_close', 'ret_high', 'ret_low', 'vol_pct', 'atr_pct']].values[-10:]
        tab_data = live_df[tabular_columns].values[-1]
        
        # Reshape for models
        X_seq_live = np.array([seq_data])
        X_tab_live = np.array([tab_data])
        
        # 3. Predict
        lstm_feat = lstm_model.predict(X_seq_live, verbose=0)
        X_comb_live = np.hstack((lstm_feat, X_tab_live))
        
        probs = xgb_model.predict_proba(X_comb_live)[0]
        
        # Apply your optimal decoupled thresholds
        if probs[1] > 0.58:
            pred_label, conf = "LONG", probs[1]
        elif probs[0] > 0.48:
            pred_label, conf = "SHORT", probs[0]
        else:
            pred_label, conf = "SKIP", max(probs)
            
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Signal: {pred_label} | Conf: {conf:.2f}")

        # 4. Push to Supabase
        supabase.table("bot_predictions").insert({
            "timestamp": int(latest_timestamp),
            "price": float(current_price),
            "prediction": pred_label,
            "probability": float(conf)
        }).execute()
        
        # Sleep until the next 15-minute mark (plus a small buffer to ensure candle is closed)
        time.sleep(60 * 15)
        
    except Exception as e:
        print(f"❌ Live Loop Error: {e}")
        time.sleep(60) # Retry in a minute if API fails
