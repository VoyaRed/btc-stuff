// bot.cjs - UpsideDownCake 24/7 Engine 🍰 v7.6.6 (Intelligence, Sync & Dynamic Whale Tracking)
// STRICT SYSTEM POLICY: THIS BOT MUST NEVER GENERATE, LOG, OR DISPLAY DELIBERATELY FAKE DATA OR MOCK PRICES.

const { ethers } = require('ethers');
const { createClient } = require('@supabase/supabase-js');
const ccxt = require('ccxt');
const { Impit } = require('impit');
const WebSocket = require('ws');

const impersonator = new Impit({ 
    browser: 'chrome',
    proxyUrl: 'http://zirrujpi-ch-city_wettingen-954674:8e2wprq017db@p.webshare.io:80' 
});

// 1. Create a dedicated fetch wrapper
const customFetch = async (url, options) => {
    try {
        return await impersonator.fetch(url, options);
    } catch (err) {
        console.error("🚨 REAL PROXY/NETWORK ERROR:", err.message);
        throw err; 
    }
};

// 2. Attach the native Response class so CCXT doesn't crash internally
customFetch.Response = Response;

// 3. Configure the exchange using the patched custom fetch
const exchange = new ccxt.binance({
    enableRateLimit: true,
    options: { defaultType: 'spot' },
    fetchImplementation: customFetch 
});

// --- CONFIG ---
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://tggqamigkruvhoqkyxrq.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_KEY || 'sb_publishable_HVa5hO_AyTxmsI_iIgrDBA_jSenZuSD';
const supabaseClient = createClient(SUPABASE_URL, SUPABASE_KEY);

const PREDICT_ADDR = "0x18B2A687610328590Bc8F2e5fEdDe3b582A49cdA";

const ABI = [
    "function currentEpoch() view returns (uint256)", 
    "function rounds(uint256) view returns (uint256 epoch, uint256 startTimestamp, uint256 lockTimestamp, uint256 closeTimestamp, int256 lockPrice, int256 closePrice, uint256 lockOracleId, uint256 closeOracleId, uint256 totalAmount, uint256 bullAmount, uint256 bearAmount, uint256 rewardBaseCalAmount, uint256 rewardAmount, bool oracleCalled)",
    "event BetBull(address indexed sender, uint256 indexed epoch, uint256 amount)",
    "event BetBear(address indexed sender, uint256 indexed epoch, uint256 amount)"
];

// --- STATE VARIABLES ---
let provider, contract;
let lastEpochChecked = 0;
let memoryStore = {};
let lastScrapeTime = 0;

let syncingEpoch = 0; 

const SCRAPE_INTERVAL = 22000; 
let localCandles = [];

// --- DYNAMIC WHALE TRACKING STATE ---
let whaleData = {
    epoch: 0,
    bullVolume: 0,
    bearVolume: 0,
    thresholdBNB: 2.0 // Starts with a 2 BNB default fallback until the first dynamic calculation
};

function startWhaleStream() {
    console.log(`🐳 Starting Dynamic Whale Tracker Layer...`);

    contract.on("BetBull", (sender, epoch, amount) => {
        const epochNum = epoch.toNumber();
        const amountBNB = parseFloat(ethers.utils.formatUnits(amount, 18));
        
        if (amountBNB >= whaleData.thresholdBNB) {
            if (whaleData.epoch !== epochNum) {
                whaleData.epoch = epochNum;
                whaleData.bullVolume = 0;
                whaleData.bearVolume = 0;
            }
            whaleData.bullVolume += amountBNB;
            console.log(`🟢 🐳 WHALE ALERT: ${amountBNB.toFixed(2)} BNB on BULL for Epoch #${epochNum} (Threshold: ${whaleData.thresholdBNB.toFixed(2)} BNB)`);
        }
    });

    contract.on("BetBear", (sender, epoch, amount) => {
        const epochNum = epoch.toNumber();
        const amountBNB = parseFloat(ethers.utils.formatUnits(amount, 18));
        
        if (amountBNB >= whaleData.thresholdBNB) {
            if (whaleData.epoch !== epochNum) {
                whaleData.epoch = epochNum;
                whaleData.bullVolume = 0;
                whaleData.bearVolume = 0;
            }
            whaleData.bearVolume += amountBNB;
            console.log(`🔴 🐳 WHALE ALERT: ${amountBNB.toFixed(2)} BNB on BEAR for Epoch #${epochNum} (Threshold: ${whaleData.thresholdBNB.toFixed(2)} BNB)`);
        }
    });
}

function startCandleStream() {
    const wsUrl = 'wss://stream.binance.com:9443/ws/bnbusdt@kline_5m';
    const ws = new WebSocket(wsUrl);

    ws.on('open', () => {
        console.log("🔌 Live WebSocket connected to Binance (BNB/USDT 5m)");
    });

    ws.on('message', (data) => {
        const message = JSON.parse(data);
        const kline = message.k; 

        if (Math.random() < 0.01) { 
            console.log(`✅ Heartbeat: Received tick for BNB/USDT. Current Close: ${kline.c}`);
        }
        
        const candle = [
            kline.t,
            parseFloat(kline.o),
            parseFloat(kline.h),
            parseFloat(kline.l),
            parseFloat(kline.c),
            parseFloat(kline.v)
        ];

        if (localCandles.length > 0 && localCandles[localCandles.length - 1][0] === candle[0]) {
            localCandles[localCandles.length - 1] = candle; 
        } else {
            localCandles.push(candle);
            if (localCandles.length > 1000) localCandles.shift(); 
        }
    });

    ws.on('error', (err) => console.error("❌ WebSocket Error:", err));
    ws.on('close', () => {
        console.log("🔌 WebSocket disconnected. Reconnecting in 5 seconds...");
        setTimeout(startCandleStream, 5000);
    });

}

async function findFastestRPC() {
    const nodes = [
        "https://bsc-rpc.publicnode.com",
        "https://binance.llamarpc.com",
        "https://bsc-dataseed.binance.org"
    ];

    for (let url of nodes) {
        try {
            const p = new ethers.providers.JsonRpcProvider(url);
            const c = new ethers.Contract(PREDICT_ADDR, ABI, p);
            await c.currentEpoch(); 
            return { provider: p, contract: c };

        } catch (e) {
            console.warn(`Node ${url} failed, trying next...`);
        }
    }
    throw new Error("All RPC nodes failed.");
}

let isInitialFetchDone = false;
let binanceSleepUntil = 0; 

async function startBot() {
    console.log("🍰 UpsideDownCake 24/7 Engine Starting...");

    if (Date.now() < binanceSleepUntil) {
        const remainingTime = Math.ceil((binanceSleepUntil - Date.now()) / 1000);
        console.log(`💤 Bot is in Sleep Mode. Skipping initialization. Waking up in ${remainingTime}s...`);
        setTimeout(startBot, 10000);
        return;
    }

    try {
        console.log("Loading Binance markets to prevent rate limits...");
        await exchange.loadMarkets();
        
        console.log("Fetching initial 1000 candles...");
        localCandles = await exchange.fetchOHLCV('BNB/USDT', '5m', undefined, 1000);
        startCandleStream();
        isInitialFetchDone = true;

        const fastest = await findFastestRPC();
        provider = fastest.provider;
        contract = fastest.contract;

        startWhaleStream();

        syncingEpoch = (await contract.currentEpoch()).toNumber();
        console.log(`✅ Connected to BSC successfully.`);
        console.log(`🔄 Startup Sync: Bot will observe but skip betting on partial Epoch #${syncingEpoch} to ensure perfect timing.`);

        runLoop();
        backgroundVerificationLoop();

    } catch (error) {
        const errorMsg = error.message || "";
        const headers = error.response?.headers || error.responseHeaders;

        if (errorMsg.includes('418')) {
            console.error(`🛑 [HTTP 418] IP Auto-Banned: Binance has temporarily banned your IP.`);
            let baseBanDurationMs = 5 * 60 * 1000; 
            
            if (headers) {
                const rawHeader = typeof headers.get === 'function' 
                    ? headers.get('retry-after') || headers.get('Retry-After') || headers.get('x-mbx-banned-until')
                    : headers['retry-after'] || headers['Retry-After'] || headers['x-mbx-banned-until'];
                    
                if (rawHeader) {
                    const parsedVal = parseInt(rawHeader, 10);
                    if (!isNaN(parsedVal)) {
                        if (parsedVal > 1000000000000) { 
                            baseBanDurationMs = Math.max(0, parsedVal - Date.now());
                        } else if (parsedVal > 1000000000) {
                            baseBanDurationMs = Math.max(0, (parsedVal * 1000) - Date.now());
                        } else {
                            baseBanDurationMs = parsedVal * 1000;
                        }
                    }
                }
            }
            const paddingMs = 5 * 60 * 1000;
            const totalSleepMs = baseBanDurationMs + paddingMs;
            binanceSleepUntil = Date.now() + totalSleepMs;
            console.error(`   -> Total Sleep Mode Duration: ${(totalSleepMs / 1000 / 60).toFixed(2)} minutes`);
            setTimeout(startBot, totalSleepMs);

        } else if (errorMsg.includes('429')) {
            console.error(`⚠️ [HTTP 429] Rate Limit Hit: Too Many Requests.`);
            let sleepDurationMs = 60 * 1000; 
            
            if (headers) {
                const rawHeader = typeof headers.get === 'function' 
                    ? headers.get('retry-after') || headers.get('Retry-After')
                    : headers['retry-after'] || headers['Retry-After'];
                    
                if (rawHeader) {
                    const retrySeconds = parseInt(rawHeader, 10);
                    if (!isNaN(retrySeconds) && retrySeconds < 1000000000) { 
                        sleepDurationMs = retrySeconds * 1000;
                    }
                }
            }
            binanceSleepUntil = Date.now() + sleepDurationMs;
            console.error(`   -> Cooling down for ${(sleepDurationMs / 1000 / 60).toFixed(2)} minutes.`);
            setTimeout(startBot, sleepDurationMs);

        } else {
            console.error(`❌ Initialization failed (Error: ${error.message}). Retrying in 10s...`);
            setTimeout(startBot, 10000);
        }
    }
}

async function runLoop() {
    try {
        await checkRound();
    } catch (error) {
        console.warn("Loop error:", error.message);
        
        // Auto-heal if the RPC Node dies
        if (error.message.includes('CALL_EXCEPTION') || error.message.includes('SERVER_ERROR') || error.message.includes('missing revert data')) {
            console.log("🔄 RPC Node failure detected. Hunting for a new healthy node...");
            try {
                const fastest = await findFastestRPC();
                provider = fastest.provider;
                
                // Re-assign the contract and reboot the whale stream listeners
                contract.removeAllListeners(); 
                contract = fastest.contract;
                startWhaleStream();
                
                console.log("✅ Successfully hot-swapped to a new RPC node.");
            } catch (e) {
                console.error("❌ Auto-heal failed. All nodes might be down.");
            }
        }
    }
    setTimeout(runLoop, 2000);
}

async function backgroundVerificationLoop() {
    try {
        const currentEpoch = (await contract.currentEpoch()).toNumber();
        if (currentEpoch > 1) {
            verifyResult(currentEpoch - 1).catch(e => console.error("Verify Error:", e));
        }

        const { data: pendingLogs } = await supabaseClient
            .from('prediction_logs')
            .select('epoch_id')
            .eq('result', 'PENDING');

        if (pendingLogs && pendingLogs.length > 0) {
            for (let log of pendingLogs) {
                if (log.epoch_id <= currentEpoch - 2) {
                    verifyResult(log.epoch_id).catch(e => console.error("Verify Error:", e));
                }
            }
        }
    } catch (error) {
        console.warn("Background Verification loop error:", error.message);
    }
    setTimeout(backgroundVerificationLoop, 15000); 
}

async function getBotSettings() {
    try {
        const { data, error } = await supabaseClient
            .from('bot_settings')
            .select('*')
            .eq('id', 1)
            .single();

        if (error) throw error;
        
        return {
            ev_threshold: data?.ev_threshold ?? 1.5,
            volatility_threshold: data?.volatility_threshold ?? 0.30,
            base_confidence: data?.base_confidence ?? 63.0,
            high_volatility_confidence: data?.high_volatility_confidence ?? 68.0,
            ema_fast_period: data?.ema_fast_period ?? 5,
            ema_slow_period: data?.ema_slow_period ?? 13,
            penalty_3_candles: data?.penalty_3_candles ?? 3.5,
            penalty_4_candles: data?.penalty_4_candles ?? 2.5,
            penalty_5_candles: data?.penalty_5_candles ?? 10.0,
            weight_macd: data?.weight_macd ?? 3.0,
            weight_rsi: data?.weight_rsi ?? 1.5,
            weight_ema: data?.weight_ema ?? 1.5,
            weight_pattern: data?.weight_pattern ?? 3.5,
            weight_history: data?.weight_history ?? 1.5,
            rvol_threshold: data?.rvol_threshold ?? 1.5,
            macro_ema_fast: data?.macro_ema_fast ?? 27,
            macro_ema_slow: data?.macro_ema_slow ?? 63,
            macro_weight: data?.macro_weight ?? 2.0
        };
    } catch (e) {
        console.warn("Could not fetch bot settings, using defaults");
        return { 
            ev_threshold: 1.5, volatility_threshold: 0.30, base_confidence: 63.0, high_volatility_confidence: 68.0,
            ema_fast_period: 5, ema_slow_period: 13, penalty_3_candles: 3.5, penalty_4_candles: 2.5, penalty_5_candles: 10.0, 
            weight_macd: 3.0, weight_rsi: 1.5, weight_ema: 1.5, weight_pattern: 3.5, weight_history: 1.5, rvol_threshold: 1.5,
            macro_ema_fast: 27, macro_ema_slow: 63, macro_weight: 2.0
        };
    }
}

async function checkRound() {
    const currentEpoch = (await contract.currentEpoch()).toNumber();
    const staleEpoch = currentEpoch - 10;

    Object.keys(memoryStore).forEach(key => {
        if (key.includes(`_${staleEpoch}`)) delete memoryStore[key];
    });

    if (currentEpoch === syncingEpoch) {
        if (!memoryStore[`sync_msg_${currentEpoch}`]) {
            console.log(`⏳ Mid-round deployment detected. Syncing data streams. Will begin trading on Epoch #${currentEpoch + 1}`);
            await supabaseClient
                .from('market_stats')
                .update({ 
                    current_pred: 'SYNCING', 
                    current_conf: 'Waiting for next round...',
                    thought_process: `Bot was recently deployed or restarted. Skipping partial Epoch #${currentEpoch} to allow WebSocket streams and timing loops to perfectly synchronize.`
                })
                .eq('id', 1);
            memoryStore[`sync_msg_${currentEpoch}`] = true;
        }
        return;
    }

    const nextRoundData = await contract.rounds(currentEpoch);
    const lockTimestamp = nextRoundData.lockTimestamp.toNumber();
    const now = Math.floor(Date.now() / 1000);
    const secondsLeft = lockTimestamp - now;

    if (secondsLeft > 102) {
        if (!memoryStore[`cleared_${currentEpoch}`]) {
            console.log(`⏳ Epoch #${currentEpoch} just started. Sleeping until 102s mark...`);

            // --- DYNAMIC WHALE THRESHOLD CALCULATION ---
            try {
                const prevRoundData = await contract.rounds(currentEpoch - 1);
                const prevBull = parseFloat(ethers.utils.formatUnits(prevRoundData.bullAmount, 18));
                const prevBear = parseFloat(ethers.utils.formatUnits(prevRoundData.bearAmount, 18));
                const prevTotal = prevBull + prevBear;

                if (prevTotal > 0) {
                    whaleData.thresholdBNB = Math.max(0.5, Math.min(5.0, prevTotal * 0.10));
                    console.log(`🔄 Dynamic Whale Threshold set to ${whaleData.thresholdBNB.toFixed(2)} BNB (10% of Epoch #${currentEpoch - 1} pool: ${prevTotal.toFixed(2)} BNB)`);
                }
            } catch (err) {
                console.warn("⚠️ Failed to parse previous round size for dynamic whale calculation. Keeping threshold at:", whaleData.thresholdBNB);
            }

            let lastAnalysis = "";
            const lastData = memoryStore[`best_${currentEpoch - 1}`];

            if (lastData && lastData.thought_process) {
                lastAnalysis = `\n\n--- LAST MARKET ANALYSIS ---\n${lastData.thought_process}`;
            }

            await supabaseClient
                .from('market_stats')
                .update({ 
                    current_pred: 'NONE', 
                    current_conf: 'Calculating...',
                    thought_process: `Waiting for initial 3-minute market settling...${lastAnalysis}`
                })
                .eq('id', 1);

            memoryStore[`cleared_${currentEpoch}`] = true;
        }
    }

    if (secondsLeft > 0 && secondsLeft <= 102 && !memoryStore[`locked_${currentEpoch}`]) {
        if (Date.now() - lastScrapeTime > SCRAPE_INTERVAL) {
            console.log(`📡 Scanning... Epoch #${currentEpoch} locks in ${secondsLeft}s`);
            await generatePrediction(currentEpoch);
            lastScrapeTime = Date.now();
        }
    }

    if (secondsLeft <= 33 && secondsLeft > 0 && !memoryStore[`locked_${currentEpoch}`]) {
        console.log(`⚡ Executing absolute final scan right before lock-in...`);
        await generatePrediction(currentEpoch);

        if (!memoryStore[`best_${currentEpoch}`]) {
            console.warn(`⚠️ Failsafe triggered: No prediction generated for #${currentEpoch}.`);
            memoryStore[`best_${currentEpoch}`] = {
                current_pred: "NONE", 
                current_conf: "binance data timeout",
                numeric: 50,
                later_pred: "NONE",
                later_conf: "0%",
                rsi: 0, macd: 0, price: 0,
                thought_process: "Emergency Fallback: Binance data retrieval timed out before lock."
            };
        }
        console.log(`⏱️ Locking in Epoch #${currentEpoch}`);
        await lockInPrediction(currentEpoch);
    }
}

async function updateMarketStats(rsi, currentMACD, currentClose, currentPred = "NONE", currentConf = "0%", laterPred = "NONE", laterConf = "0%", ThoughtProcess = "", currentRegime = "UNKNOWN") {
    const { error } = await supabaseClient
        .from('market_stats')
        .upsert([{ 
            id: 1, 
            rsi: rsi, 
            macd: currentMACD, 
            price: currentClose,
            current_pred: currentPred,
            current_conf: currentConf,
            later_pred: laterPred,
            later_conf: laterConf,
            thought_process: ThoughtProcess,
            updated_at: new Date().toISOString() 
        }]);

    if (error) console.error("Error updating stats:", error);
}

async function generatePrediction(targetEpoch) {
    try {
        memoryStore[`pred_${targetEpoch}`] = "PENDING";
        const settings = await getBotSettings();
        let candles = localCandles;
        
        if (!candles || candles.length < 50) {
            console.log("⚠️ Live stream not ready, falling back to REST API...");
            candles = await exchange.fetchOHLCV('BNB/USDT', '5m', undefined, 1000);
        }

        if (!Array.isArray(candles) || candles.length < 50) {
            throw new Error("Insufficient candles returned from Binance.");
        }
        
        const opens = candles.map(c => parseFloat(c[1]));
        const highs = candles.map(c => parseFloat(c[2]));
        const lows = candles.map(c => parseFloat(c[3]));
        const closes = candles.map(c => parseFloat(c[4]));
        const volumes = candles.map(c => parseFloat(c[5])); 
        const currentClose = closes[closes.length - 1];

        // RSI Array Calculations
        let gains = [], losses = [];

        for (let i = 1; i < closes.length; i++) {
            let diff = closes[i] - closes[i - 1];
            gains.push(diff > 0 ? diff : 0);
            losses.push(diff < 0 ? Math.abs(diff) : 0);
        }
        
        let rsiHistory = [];
        let avgGain = gains.slice(0, 14).reduce((a, b) => a + b, 0) / 14;
        let avgLoss = losses.slice(0, 14).reduce((a, b) => a + b, 0) / 14;

        rsiHistory.push(avgLoss === 0 ? 100 : 100 - (100 / (1 + (avgGain / avgLoss))));

        for (let i = 14; i < gains.length; i++) {
            avgGain = ((avgGain * 13) + gains[i]) / 14;
            avgLoss = ((avgLoss * 13) + losses[i]) / 14;
            let currentRsi = 100;
            if (avgLoss !== 0) currentRsi = 100 - (100 / (1 + (avgGain / avgLoss)));
            else if (avgGain === 0) currentRsi = 0;
            rsiHistory.push(currentRsi);
        }

        let rsi = rsiHistory[rsiHistory.length - 1];
        let previousRSI_3_candles_ago = rsiHistory[rsiHistory.length - 4] || rsi;
        let rsiSlope = (rsi - previousRSI_3_candles_ago) / 3;

        let previousRSI_1_candle_ago = rsiHistory[rsiHistory.length - 2] || rsi;
        let previousRSI_4_candles_ago = rsiHistory[rsiHistory.length - 5] || previousRSI_3_candles_ago;
        let previousRSISlope = (previousRSI_1_candle_ago - previousRSI_4_candles_ago) / 3;
        let rsiAcceleration = rsiSlope - previousRSISlope;

        const bbPeriod = 20;
        const bbSlice = closes.slice(-bbPeriod);
        const sma = bbSlice.reduce((a, b) => a + b, 0) / bbPeriod;
        const variance = bbSlice.reduce((acc, val) => acc + Math.pow(val - sma, 2), 0) / bbPeriod;
        const stdDev = Math.sqrt(variance);
        const upperBB = sma + (stdDev * 2);
        const lowerBB = sma - (stdDev * 2);

        const calculateEMAArray = (data, period) => {
            const k = 2 / (period + 1);
            let emaArray = [data[0]]; 
            for (let i = 1; i < data.length; i++) {
                emaArray.push((data[i] * k) + (emaArray[i - 1] * (1 - k)));
            }
            return emaArray;
        };

        const emaFast = calculateEMAArray(closes, settings.ema_fast_period)[closes.length - 1];
        const emaSlow = calculateEMAArray(closes, settings.ema_slow_period)[closes.length - 1];
        const macroEmaFast = calculateEMAArray(closes, settings.macro_ema_fast)[closes.length - 1];
        const macroEmaSlow = calculateEMAArray(closes, settings.macro_ema_slow)[closes.length - 1];
        
        const ema12Array = calculateEMAArray(closes, 12);
        const ema26Array = calculateEMAArray(closes, 26);
        const macdLineArray = ema12Array.map((v, i) => v - ema26Array[i]);
        const signalLineArray = calculateEMAArray(macdLineArray, 9);

        const currentMACD = macdLineArray[macdLineArray.length - 1];
        const currentSignal = signalLineArray[signalLineArray.length - 1];
        const currentHist = currentMACD - currentSignal;
        const prevHist = (macdLineArray[macdLineArray.length - 2] - signalLineArray[signalLineArray.length - 2]);

        const volSMA20 = volumes.slice(-20).reduce((a,b)=>a+b,0) / 20;
        const currentVol = volumes[volumes.length - 1];
        const rvol = currentVol / volSMA20;

        let recentUps = 0, recentDowns = 0;
        const roundPromises = [];
        for(let i=1; i<=5; i++) {
            roundPromises.push(contract.rounds(targetEpoch - i).catch(() => null));
        }
        const pastRounds = await Promise.all(roundPromises);

        pastRounds.forEach(r => {
            if (r && r.oracleCalled) {
                const lp = parseFloat(ethers.utils.formatUnits(r.lockPrice, 8));
                const cp = parseFloat(ethers.utils.formatUnits(r.closePrice, 8));
                if (cp > lp) recentUps++;
                else if (cp < lp) recentDowns++;
            }
        });

        

        // --- EXISTING NODE BOT LOGIC STARTS HERE ---
        let upScore = 0, downScore = 0;
        let brainText = []; 
        let historyScore = { up: 0, down: 0 };
        let trendScore = { up: 0, down: 0 };
        let volScore = { up: 0, down: 0 };
        let patternScore = { up: 0, down: 0 };
        let whaleScore = { up: 0, down: 0 };

        let trSum = 0;
        for (let i = closes.length - 14; i < closes.length; i++) {
            const highLow = highs[i] - lows[i];
            const highClose = Math.abs(highs[i] - closes[i-1]);
            const lowClose = Math.abs(lows[i] - closes[i-1]);
            trSum += Math.max(highLow, highClose, lowClose);
        }
        const atrPercentage = ((trSum / 14) / currentClose) * 100;
        let bbWidth = (upperBB - lowerBB) / sma;

        const prevOpen = opens[opens.length - 2];
        const prevClose = closes[closes.length - 2];
        const prevHigh = highs[highs.length - 2];
        const prevLow = lows[lows.length - 2];
        
        const upperWick = prevHigh - Math.max(prevOpen, prevClose);
        const lowerWick = Math.min(prevOpen, prevClose) - prevLow;
        const bodySize = Math.max(Math.abs(prevClose - prevOpen), 0.0001);
        const roc3 = ((currentClose - closes[closes.length - 4]) / closes[closes.length - 4]) * 100;

        const isDoji = (upperWick > bodySize * 2) && (lowerWick > bodySize * 2);

// 🟢 INJECT ML SHADOW MODE HERE 🟢
        // FIX 1: Define the variable OUTSIDE the if-block so the whole function can access it
        let mlShadowLog = "🤖 [ML SHADOW MODE] API Offline, Failed, or Waiting for Data."; 

        // 1. Guard Clause: Don't call the API if we don't have enough data yet!
        if (localCandles && localCandles.length >= 10) {
            
            const mlPayload = {
                // Safely grabs exactly the last 10 candles
                candles: localCandles.slice(-10).map(c => [c[1], c[2], c[3], c[4], c[5]]),
                rsi: rsi,
                rsi_slope: rsiSlope,
                rsi_accel: rsiAcceleration,
                macd: currentMACD,
                macd_signal: currentSignal,
                macd_hist: currentHist,
                ema_fast: emaFast,
                ema_slow: emaSlow,
                macro_ema_fast: macroEmaFast,
                macro_ema_slow: macroEmaSlow,
                bb_width: bbWidth,
                atr: atrPercentage,
                rvol: rvol,
                roc3: roc3,
                body_size: bodySize,
                upper_wick: upperWick,
                lower_wick: lowerWick
            };
            
            try {
                const mlResponse = await fetch('https://python-lstm-bot.onrender.com/predict', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(mlPayload)
                });

                if (mlResponse.ok) {
                    const mlData = await mlResponse.json();
                    mlShadowLog = `🤖 [ML SHADOW MODE] Python Model predicts: ${mlData.prediction} (${mlData.confidence.toFixed(1)}%).`;
                    console.log(mlShadowLog); // Log the successful prediction
                } else {
                    // FIX 2: Capture and print the EXACT error message from Python
                    const errorDetails = await mlResponse.text();
                    console.warn(`⚠️ ML API crashed (500). Python says: ${errorDetails}`);
                }
            } catch (err) {
                console.warn(`⚠️ ML Shadow API skipped this round: ${err.message}`);
            }

        } else {
            console.log("🤖 [ML SHADOW MODE] Waiting for 10 candles to accumulate... Current count: " + (localCandles ? localCandles.length : 0));
        }
        // 🟢 END ML SHADOW MODE 🟢
        
        let colorFlips = 0;
        for (let i = closes.length - 1; i >= closes.length - 4; i--) {
            const currentColor = closes[i] >= opens[i] ? 'green' : 'red';
            const prevColor = closes[i-1] >= opens[i-1] ? 'green' : 'red';
            if (currentColor !== prevColor) {
                colorFlips++;
            }
        }
        
        const isWoodchipper = colorFlips >= 3 && atrPercentage >= parseFloat(settings.volatility_threshold);
        let currentRegime = isWoodchipper ? 'Sniper' : 'Momentum';

        if (isWoodchipper) {
            brainText.push(`🎯 [REGIME ENGAGED: SNIPER - WOODCHIPPER WHIPSAW] High volatility alternating chops detected. Fading extremes and requiring massive confirmation.`);
            settings.weight_rsi = 3.5;
            settings.weight_macd = 1.0;
        } else {
            brainText.push("🚀 [REGIME ENGAGED: BALANCED MOMENTUM] Structural clarity detected. Riding the trends smoothly.");
            settings.weight_macd = 3.5;
            settings.weight_rsi = 1.5;
        }

        // --- WHALE TRACKING INJECTION ---
        if (whaleData.epoch === targetEpoch) {
            const WHALE_MULTIPLIER = 5.0;
            if (whaleData.bullVolume > 0 || whaleData.bearVolume > 0) {
                brainText.push(`🐳 Whale Activity Detected -> UP: ${whaleData.bullVolume.toFixed(2)} BNB | DOWN: ${whaleData.bearVolume.toFixed(2)} BNB.`);
            }

            if (whaleData.bullVolume > whaleData.bearVolume * 1.5) {
                whaleScore.up += WHALE_MULTIPLIER;
                brainText.push("Massive smart money is stacking the BULL pool. Adding whale conviction (+5).");
            } else if (whaleData.bearVolume > whaleData.bullVolume * 1.5) {
                whaleScore.down += WHALE_MULTIPLIER;
                brainText.push("Massive smart money is stacking the BEAR pool. Adding whale conviction (+5).");
            }
        }

        let dynamicCeiling = Math.min(85, Math.max(72, 72 + (atrPercentage * 50)));
        let dynamicFloor = Math.max(15, Math.min(28, 28 - (atrPercentage * 50)));

        if (rsiSlope > 1.5) {
            brainText.push("RSI is aggressively rising; momentum is strong.");
            if (rsi > dynamicCeiling) {
                volScore.down += 4.0;
                brainText.push(`Warning: RSI (${rsi.toFixed(1)}) exceeded dynamic ceiling (${dynamicCeiling.toFixed(1)}). Anticipating a bearish exhaustion reversal.`);
            } else {
                trendScore.up += parseFloat(settings.weight_rsi);
            }
        } else if (rsiSlope < -1.5) {
            brainText.push("RSI is skyrocketing downward; bearish momentum is accelerating.");
            if (rsi < dynamicFloor) {
                volScore.up += 4.0;
                brainText.push(`Warning: RSI (${rsi.toFixed(1)}) breached dynamic floor (${dynamicFloor.toFixed(1)}). Anticipating a bullish exhaustion reversal.`);
            } else {
                trendScore.down += parseFloat(settings.weight_rsi);
            }
        } else if (rsiAcceleration < 0 && rsi > 60) {
            brainText.push("Warning: RSI rise is slowing down; potential overbought reversal.");
            volScore.down += (parseFloat(settings.weight_rsi) / 2);
        }

        if (recentUps >= 3) { 
            historyScore.up += parseFloat(settings.weight_history);
            brainText.push("Recent historical rounds lean bullish."); 
        }
        if (recentUps === 5) historyScore.up += (parseFloat(settings.weight_history) * 1.5);

        if (recentDowns >= 3) { 
            historyScore.down += parseFloat(settings.weight_history);
            brainText.push("Recent historical rounds lean bearish.");
        }
        if (recentDowns === 5) historyScore.down += (parseFloat(settings.weight_history) * 1.5);

        if (isWoodchipper) { 
            if (emaFast > emaSlow && currentHist > 0) {
                trendScore.up += parseFloat(settings.weight_ema);
                brainText.push("Fast EMA leads Slow EMA with positive MACD histogram. Proceeding with bullish confirmation.");
            } else if (emaFast < emaSlow && currentHist < 0) {
                trendScore.down += parseFloat(settings.weight_ema);
                brainText.push("Fast EMA trails Slow EMA with negative MACD histogram. Proceeding with bearish confirmation.");
            } else {
                brainText.push("MACD histogram does not definitively confirm the trend direction. Skipping strong directional bets.");
            }
            
            if (isDoji) {
                brainText.push("Previous candle was a Doji/Spinning Top. Ignoring directional wick patterns due to market indecision.");
            } else {
                if (upperWick > bodySize * 2) patternScore.down += parseFloat(settings.weight_pattern);
                if (lowerWick > bodySize * 2) patternScore.up += parseFloat(settings.weight_pattern);
            }
        } else {
            // FIX: Dampen the lagging 15m Macro Trend weight by 50%
            const mWeight = parseFloat(settings.macro_weight) * 0.5;

            if (macroEmaFast > macroEmaSlow && emaFast > emaSlow) {
                trendScore.up += mWeight;
                brainText.push(`Macro Trend (15m) aligns bullishly (Dampened weight: +${mWeight}).`);
            } else if (macroEmaFast < macroEmaSlow && emaFast < emaSlow) {
                trendScore.down += mWeight;
                brainText.push(`Macro Trend (15m) aligns bearishly (Dampened weight: +${mWeight}).`);
            } else {
                trendScore.up -= mWeight;
                trendScore.down -= mWeight;
                brainText.push(`Macro Trend (15m) disagrees with local 5m trend. Applying dampened penalty (-${mWeight}).`);
            }

            if (emaFast > emaSlow) { 
                trendScore.up += parseFloat(settings.weight_ema);
                brainText.push(`Fast EMA(${settings.ema_fast_period}) leads Slow EMA(${settings.ema_slow_period}) (Bullish configuration).`);
            }
            if (emaFast < emaSlow) { 
                trendScore.down += parseFloat(settings.weight_ema);
                brainText.push(`Fast EMA(${settings.ema_fast_period}) trails Slow EMA(${settings.ema_slow_period}) (Bearish configuration).`); 
            }
            
            if (currentMACD > currentSignal && currentHist > prevHist) { 
                trendScore.up += parseFloat(settings.weight_macd);
                brainText.push("MACD histogram is expanding upward, displaying strong structural expansion.");
            }
            if (currentMACD < currentSignal && currentHist < prevHist) { 
                trendScore.down += parseFloat(settings.weight_macd);
                brainText.push("MACD histogram is expanding downward, displaying strong structural compression.");
            }
            
            if (roc3 > 0.15) {
                trendScore.up += 1.5;
                brainText.push("Rate of Change (3 candles) is strongly positive. Added structural momentum (+1.5).");
            }
            if (roc3 < -0.15) {
                trendScore.down += 1.5;
                brainText.push("Rate of Change (3 candles) is strongly negative. Added structural momentum (+1.5).");
            }

            if (isDoji) {
                brainText.push("Previous candle was a Doji/Spinning Top. Ignoring directional wick patterns due to market indecision.");
            } else {
                if (upperWick > bodySize * 2) { 
                    patternScore.down += parseFloat(settings.weight_pattern);
                    brainText.push("Spotted a long upper wick on the previous candle, predicting supply overhead.");
                }
                if (lowerWick > bodySize * 2) { 
                    patternScore.up += parseFloat(settings.weight_pattern);
                    brainText.push("Spotted a long lower wick on the previous candle, predicting clear demand protection.");
                }
            }

            if (rsi < 35 && rsiSlope < -1.0 && currentHist < prevHist) {
                 trendScore.down += (parseFloat(settings.weight_macd) * 1.5);
                 brainText.push("Severe momentum drop confirmed by MACD and RSI. Applying heavy trend override.");
            } else if (rsi > 65 && rsiSlope > 1.0 && currentHist > prevHist) {
                 trendScore.up += (parseFloat(settings.weight_macd) * 1.5);
                 brainText.push("Severe momentum spike confirmed by MACD and RSI. Applying heavy trend override.");
            }
        }

        upScore = historyScore.up + trendScore.up + volScore.up + patternScore.up + whaleScore.up;
        downScore = historyScore.down + trendScore.down + volScore.down + patternScore.down + whaleScore.down;

        let netScore = Math.abs(upScore - downScore);

        if (isNaN(netScore)) netScore = 0;

        if (upScore === downScore) {
            brainText.push("Data is perfectly tied. Using directional EMA trend alignment as the structural tie-breaker.");
            if (emaFast >= emaSlow) { 
                upScore += parseFloat(settings.weight_ema);
            } else { 
                downScore += parseFloat(settings.weight_ema);
            }
            netScore = Math.abs(upScore - downScore);
        }
        
        let currentPred = (upScore > downScore) ? "UP" : "DOWN";
        let intendedSide = currentPred;

        // FIX: The RSI Momentum Veto
        if (currentPred === "UP" && rsiSlope < -1.5) {
            currentPred = "SKIP";
            brainText.push("🛑 VETO: RSI is skyrocketing downward. Canceling UP prediction to avoid catching a falling knife.");
        } else if (currentPred === "DOWN" && rsiSlope > 1.5) {
            currentPred = "SKIP";
            brainText.push("🛑 VETO: RSI is aggressively rising. Canceling DOWN prediction to avoid fighting severe momentum.");
        } else {
            brainText.push(`Conclusion: The aggregate weight of the technical data firmly favors ${currentPred}.`);
        }

        console.log(`📊 Category Breakdown [Target #${targetEpoch}] -> History: U:${historyScore.up.toFixed(1)}/D:${historyScore.down.toFixed(1)} | Trend: U:${trendScore.up.toFixed(1)}/D:${trendScore.down.toFixed(1)} | Volatility/BB: U:${volScore.up.toFixed(1)}/D:${volScore.down.toFixed(1)} | Patterns: U:${patternScore.up.toFixed(1)}/D:${patternScore.down.toFixed(1)} | Whales: U:${whaleScore.up.toFixed(1)}/D:${whaleScore.down.toFixed(1)}`);

        // FIX: The Confidence Paradox. Lowered baseline and reduced multiplier.
        let numericConfidence = 50 + (netScore * 1.5);

        if (rvol >= parseFloat(settings.rvol_threshold)) {
             brainText.push(`Volume Confirmation: RVOL is high (${rvol.toFixed(2)}x average). Amplifying confidence.`);
             numericConfidence += 2.5; 
        }

        let consecutiveCandles = 0;
        let lastColor = null;

        if (localCandles && localCandles.length >= 10) {
            for(let i = 1; i <= 8; i++) {
                const c = localCandles[localCandles.length - 1 - i];
                const o = c[1], h = c[2], l = c[3], cl = c[4];
                
                const body = Math.abs(cl - o);
                const upWick = h - Math.max(o, cl);
                const dnWick = Math.min(o, cl) - l;

                const isSpinningTop = (upWick > body * 0.5 && dnWick > body * 0.5) || body === 0;

                if (isSpinningTop) continue;

                const color = cl > o ? 'green' : 'red';

                if (lastColor === null) {
                   lastColor = color;
                   consecutiveCandles = 1;
                } else if (color === lastColor) {
                   consecutiveCandles++;
                } else {
                   break;
                }
            }
        }

       if (consecutiveCandles >= 5) {
            numericConfidence -= parseFloat(settings.penalty_5_candles);
            brainText.push(`Warning: Extreme Trend Exhaustion detected (${consecutiveCandles} consecutive identical candles). Reducing confidence by ${settings.penalty_5_candles}%.`);

        } else if (consecutiveCandles === 4) {
            numericConfidence -= parseFloat(settings.penalty_4_candles);
            brainText.push(`Caution: Late-stage trend detected (4 consecutive candles). Profit-taking is highly likely. Reducing confidence by ${settings.penalty_4_candles}%.`);

        } else if (consecutiveCandles === 3) {
            numericConfidence += parseFloat(settings.penalty_3_candles);
            brainText.push(`Momentum Confirmed: 3 consecutive identical candles. Trusting the established trend and adding ${settings.penalty_3_candles}% confidence.`);
        }

        if (isDoji) {
            numericConfidence -= 3.0;
            brainText.push(`Previous candle was a Doji. Reducing confidence slightly (-3%) to account for potential pivot risk.`);
        }
        
        numericConfidence = Math.max(0, Math.min(99.9, numericConfidence));
        let finalConfidence = numericConfidence.toFixed(1) + "%";
        let displayConf = finalConfidence;

        const VOLATILITY_THRESHOLD = parseFloat(settings.volatility_threshold);
        let requiredConfidence = parseFloat(settings.base_confidence);

        if (isWoodchipper) {
            requiredConfidence = parseFloat(settings.high_volatility_confidence);
            brainText.push(`Market Context: Woodchipper Volatility detected. Increased confidence threshold to ${requiredConfidence}%.`);
        } else if (atrPercentage > VOLATILITY_THRESHOLD) {
             requiredConfidence = parseFloat(settings.high_volatility_confidence);
             brainText.push(`Market Context: High ATR (${atrPercentage.toFixed(2)}%). Threshold set to ${requiredConfidence}%.`);
        } else {
            brainText.push(`Market Context: Normal Volatility (${atrPercentage.toFixed(2)}%). Threshold set to ${requiredConfidence}%.`);
        }
        
        console.log(`📊 Market Volatility Audit | ATR%: ${atrPercentage.toFixed(2)}% | Threshold: ${VOLATILITY_THRESHOLD}% | Required Conf: ${requiredConfidence}%`);

        if (numericConfidence < requiredConfidence || currentPred === "SKIP") {
            if (numericConfidence < requiredConfidence && currentPred !== "SKIP") {
                brainText.push(`Confidence (${finalConfidence}) is below the required ${requiredConfidence}%. Executing SKIP to avoid low-edge trades.`);
            }
            currentPred = "SKIP";
            displayConf = `${finalConfidence} (Target: ${requiredConfidence}%) (Try: ${intendedSide})`;
        } else {
            brainText.push(`Confidence (${finalConfidence}) meets threshold (${requiredConfidence}%). Proceeding.`);
            displayConf = `${finalConfidence} (Target: ${requiredConfidence}%)`; 
        }

        // INJECT ML SHADOW LOG HERE
        brainText.push(mlShadowLog);

        const ThoughtProcess = brainText.join(" ");
        
        let laterUpProb = 50 + (emaFast > emaSlow ? 10 : -10) + ((rsi - 50) * 0.4) + (recentUps > recentDowns ? 5 : -5);
        if (isNaN(laterUpProb)) laterUpProb = 50; 
        laterUpProb = Math.max(10, Math.min(90, laterUpProb));
        let laterDownProb = 100 - laterUpProb;
        let laterPred = laterUpProb > 50 ? "UP" : "DOWN";
        let laterMajorityProb = Math.max(laterUpProb, laterDownProb).toFixed(1);
        console.log(`🔥 Live Scan Update! Direction: ${currentPred} | current_conf: ${displayConf}`);
        
        memoryStore[`best_${targetEpoch}`] = {
            current_pred: currentPred,
            current_conf: displayConf,
            numeric: (numericConfidence - 1),
            later_pred: laterPred,
            later_conf: laterMajorityProb,
            rsi: rsi,
            macd: currentMACD,
            price: currentClose,
            market_regime: currentRegime,
            thought_process: ThoughtProcess 
        };
        await updateMarketStats(rsi, currentMACD, currentClose, currentPred, displayConf, laterPred, laterMajorityProb, ThoughtProcess, currentRegime);

    } catch (e) {
        console.error("Brain Failed:", e);
    }
}

async function lockInPrediction(targetEpoch) {
    const bestData = memoryStore[`best_${targetEpoch}`];
    if (!bestData || bestData.numeric === -1) return;

    memoryStore[`locked_${targetEpoch}`] = true;
    console.log(`\n🔒 ROUND LIVE! Locking in best prediction for Epoch #${targetEpoch}: ${bestData.current_pred} (${bestData.current_conf})`);

    // FIX: Webhook threshold updated to 75.0%
    if (bestData.numeric >= 75.0 && bestData.current_pred !== "SKIP") {
        const webhookUrl = "https://discord.com/api/webhooks/1520463983998537800/T1xaGGZJ7YA_aw7JnbVKkyf9HwWta8D3W3VbuDhw5_vEiBtrqKqnzG37VIKH9WcwABx8";
        fetch(webhookUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                username: "Cake Alert Bot 🍰",
                content: `🚨 **High Confidence Alert!** 🚨\nEpoch: #${targetEpoch}\nPrediction: **${bestData.current_pred}**\nConfidence: **${bestData.current_conf}**`
            })
        }).catch(err => console.error("Failed to send webhook:", err));
    }
    
    const { error } = await supabaseClient.from('prediction_logs').upsert([{ 
        epoch_id: targetEpoch, 
        predicted_side: bestData.current_pred, 
        result: 'PENDING',
        confidence: bestData.current_conf,
        is_locked: true,
        thought_process: bestData.thought_process,
    }], { 
        onConflict: 'epoch_id' 
    });

    if (error) console.error("❌ Early Supabase insert error:", error);
    await updateMarketStats(bestData.rsi, bestData.macd, bestData.price, "NONE", "Calculating...", bestData.later_pred, bestData.later_conf, bestData.thought_process);

    setTimeout(async () => {
        try {
            const lockedRoundData = await contract.rounds(targetEpoch);
            const bullPool = parseFloat(ethers.utils.formatUnits(lockedRoundData.bullAmount, 18));
            const bearPool = parseFloat(ethers.utils.formatUnits(lockedRoundData.bearAmount, 18));
            const totalPool = bullPool + bearPool;

            if (totalPool > 0) {
                const rewardPool = totalPool * 0.97;
                let lockedMultiplier = 0;
                let intendedSide = bestData.current_pred;

                if (intendedSide.startsWith("SKIP")) {
                    if (bestData.current_conf && bestData.current_conf.includes("Try: UP")) intendedSide = "UP";
                    else if (bestData.current_conf && bestData.current_conf.includes("Try: DOWN")) intendedSide = "DOWN";
                }

                if (intendedSide === "UP" && bullPool > 0) {
                    lockedMultiplier = rewardPool / bullPool;
                } else if (intendedSide === "DOWN" && bearPool > 0) {
                    lockedMultiplier = rewardPool / bearPool;
                }

                console.log(`\n💸 [Epoch ${targetEpoch}] Live! Confirmed payout for ${intendedSide}: ${lockedMultiplier.toFixed(2)}x`);
                await supabaseClient
                    .from('prediction_logs')
                    .update({ payout_multiplier: Number(lockedMultiplier.toFixed(2)) })
                    .eq('epoch_id', targetEpoch);
            }
        } catch (err) {
            console.warn(`Could not fetch confirmed pool sizes for Epoch ${targetEpoch}:`, err.message);
        }
    }, 5000);
}

async function verifyResult(epochToCheck) {
    try {
        let round = await contract.rounds(epochToCheck);
        if (!round.oracleCalled) return;
        
        const lockPrice = parseFloat(ethers.utils.formatUnits(round.lockPrice, 8));
        const closePrice = parseFloat(ethers.utils.formatUnits(round.closePrice, 8));
        
        let actualResult;

        if (closePrice === lockPrice) {
            actualResult = "TIE";
        } else {
            actualResult = closePrice > lockPrice ? "UP" : "DOWN"; 
        }
        
        const { data, error: fetchError } = await supabaseClient
            .from('prediction_logs')
            .select('*')
            .eq('epoch_id', epochToCheck)
            .single();

        if (fetchError || !data) return;
        if (data.result !== 'PENDING') return;
        
        let resultStatus;
        if (actualResult === "TIE") {
            resultStatus = "TIE";
        } else if (data.predicted_side.startsWith("SKIP")) {
            let originalSide = null;
            if (data.confidence && data.confidence.includes("Try: UP")) originalSide = "UP";
            else if (data.confidence && data.confidence.includes("Try: DOWN")) originalSide = "DOWN";

            if (originalSide) {
                resultStatus = (originalSide === actualResult) ? "SKIP/WIN" : "SKIP/LOSS";
            } else {
                resultStatus = "SKIP/" + actualResult;
            }
        } else {
            resultStatus = (data.predicted_side === actualResult) ? "WIN" : "LOSS"; 
        }

        console.log(`\n⚖️ [Epoch ${epochToCheck}] Resolving... Result: ${resultStatus}`);

        let finalMultiplier = 0;
        const bullPool = parseFloat(ethers.utils.formatUnits(round.bullAmount, 18));
        const bearPool = parseFloat(ethers.utils.formatUnits(round.bearAmount, 18));
        const totalPool = bullPool + bearPool;

        if (totalPool > 0) {
            const rewardPool = totalPool * 0.97;
            let intendedSide = data.predicted_side;
            
            if (intendedSide.startsWith("SKIP")) {
                if (data.confidence && data.confidence.includes("Try: UP")) intendedSide = "UP";
                else if (data.confidence && data.confidence.includes("Try: DOWN")) intendedSide = "DOWN";
            }

            if (intendedSide === "UP" && bullPool > 0) {
                finalMultiplier = rewardPool / bullPool;
            } else if (intendedSide === "DOWN" && bearPool > 0) {
                finalMultiplier = rewardPool / bearPool;
            }
        }

        const { error: updateError } = await supabaseClient
            .from('prediction_logs')
            .update({ 
                result: resultStatus,
                payout_multiplier: Number(finalMultiplier.toFixed(2)) 
            })
            .eq('epoch_id', epochToCheck);
            
        if (updateError) {
            console.error(`❌ Supabase Update Error for Epoch ${epochToCheck}:`, updateError.message);
            return;
        }

        const { data: recentLogs } = await supabaseClient
            .from('prediction_logs')
            .select('result, confidence')
            .in('result', ['WIN', 'LOSS', 'SKIP/UP', 'SKIP/DOWN', 'SKIP/WIN', 'SKIP/LOSS'])
            .order('epoch_id', { ascending: false })
            .limit(15);

        if (recentLogs && recentLogs.length > 0) {
            const mixedWins = recentLogs.filter(l => l.result === 'WIN' || l.result === 'SKIP/UP' || l.result === 'SKIP/WIN').length;
            const mixedRate = ((mixedWins / recentLogs.length) * 100).toFixed(1);

            const trendLogs = recentLogs.filter(l => {
                const match = l.confidence.match(/(\d+(?:\.\d+)?)/);
                return match ? parseFloat(match[1]) >= 55.0 : false;
            });

            const trendWins = trendLogs.filter(l => l.result === 'WIN' || l.result === 'SKIP/UP' || l.result === 'SKIP/WIN').length;
            const trendRate = trendLogs.length > 0 ? ((trendWins / trendLogs.length) * 100).toFixed(1) : "0.0";

            console.log(`📈 Mixed Market (Overall Average): ${mixedRate}%`);
            console.log(`🚀 Trend Market (Conviction > 55%): ${trendRate}%`);
        }
    } catch(e) { 
        console.error("Result Verification Failed:", e);
    }
}

startBot();

const http = require('http');
http.createServer((req, res) => { res.writeHead(200); res.end('Bot running'); }).listen(process.env.PORT || 3000);
