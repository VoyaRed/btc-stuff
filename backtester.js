// backtest.js - True Historical Forward/Backtester
const ccxt = require('ccxt');

// Your static weights from Supabase
const settings = {
    ema_fast_period: 5, ema_slow_period: 13,
    macro_ema_fast: 27, macro_ema_slow: 63,
    weight_macd: 3.0, weight_rsi: 1.5, weight_ema: 2.0, 
    weight_pattern: 2.5, weight_history: 1.0, macro_weight: 1.5,
    penalty_3_candles: 2.0, penalty_4_candles: 3.0, penalty_5_candles: 10.0,
    rvol_threshold: 1.5, volatility_threshold: 0.14,
    base_confidence: 63.0, high_volatility_confidence: 68.0
};

// Math Helpers from your bot.cjs
const calculateEMAArray = (data, period) => {
    const k = 2 / (period + 1);
    let emaArray = [data[0]];
    for (let i = 1; i < data.length; i++) {
        emaArray.push((data[i] * k) + (emaArray[i - 1] * (1 - k)));
    }
    return emaArray;
};

const calculateRSI = (closes) => {
    let gains = [], losses = [];
    for (let i = 1; i < closes.length; i++) {
        let diff = closes[i] - closes[i - 1];
        gains.push(diff > 0 ? diff : 0);
        losses.push(diff < 0 ? Math.abs(diff) : 0);
    }
    let avgGain = gains.slice(0, 14).reduce((a, b) => a + b, 0) / 14;
    let avgLoss = losses.slice(0, 14).reduce((a, b) => a + b, 0) / 14;
    let rsi = avgLoss === 0 ? 100 : 100 - (100 / (1 + (avgGain / avgLoss)));
    for (let i = 14; i < gains.length; i++) {
        avgGain = ((avgGain * 13) + gains[i]) / 14;
        avgLoss = ((avgLoss * 13) + losses[i]) / 14;
        rsi = avgLoss === 0 ? 100 : (avgGain === 0 ? 0 : 100 - (100 / (1 + (avgGain / avgLoss))));
    }
    return rsi;
};

// Core Engine Simulator
function simulatePrediction(candles) {
    if (candles.length < 50) return { pred: "SKIP", conf: 0 };

    const opens = candles.map(c => parseFloat(c[1]));
    const highs = candles.map(c => parseFloat(c[2]));
    const lows = candles.map(c => parseFloat(c[3]));
    const closes = candles.map(c => parseFloat(c[4]));
    const volumes = candles.map(c => parseFloat(c[5]));

    const currentClose = closes[closes.length - 1];
    const prevOpen = opens[opens.length - 2];
    const prevClose = closes[closes.length - 2];
    const prevHigh = highs[highs.length - 2];
    const prevLow = lows[lows.length - 2];

    // Indicators
    const rsi = calculateRSI(closes);
    const emaFast = calculateEMAArray(closes, settings.ema_fast_period).pop();
    const emaSlow = calculateEMAArray(closes, settings.ema_slow_period).pop();
    const macroEmaFast = calculateEMAArray(closes, settings.macro_ema_fast).pop();
    const macroEmaSlow = calculateEMAArray(closes, settings.macro_ema_slow).pop();
    
    const ema12 = calculateEMAArray(closes, 12);
    const ema26 = calculateEMAArray(closes, 26);
    const macdLine = ema12.map((v, i) => v - ema26[i]);
    const signalLine = calculateEMAArray(macdLine, 9);
    const currentMACD = macdLine[macdLine.length - 1];
    const currentSignal = signalLine[signalLine.length - 1];
    const currentHist = currentMACD - currentSignal;
    const prevHist = macdLine[macdLine.length - 2] - signalLine[signalLine.length - 2];

    const volSMA20 = volumes.slice(-20).reduce((a,b)=>a+b,0) / 20;
    const rvol = volumes[volumes.length - 1] / volSMA20;

    let trSum = 0;
    for (let i = closes.length - 14; i < closes.length; i++) {
        trSum += Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i-1]), Math.abs(lows[i] - closes[i-1]));
    }
    const atrPercentage = ((trSum / 14) / currentClose) * 100;

    const upperWick = prevHigh - Math.max(prevOpen, prevClose);
    const lowerWick = Math.min(prevOpen, prevClose) - prevLow;
    const bodySize = Math.max(Math.abs(prevClose - prevOpen), 0.0001);
    const isDoji = (upperWick > bodySize * 2) && (lowerWick > bodySize * 2);

    let upScore = 0, downScore = 0;

    // Macro Trend Logic
    const mWeight = settings.macro_weight * 0.5;
    if (macroEmaFast > macroEmaSlow && emaFast > emaSlow) upScore += mWeight;
    else if (macroEmaFast < macroEmaSlow && emaFast < emaSlow) downScore += mWeight;
    else { upScore -= mWeight; downScore -= mWeight; }

    // Micro Trend & MACD Logic
    if (emaFast > emaSlow) upScore += settings.weight_ema;
    if (emaFast < emaSlow) downScore += settings.weight_ema;
    if (currentMACD > currentSignal && currentHist > prevHist) upScore += settings.weight_macd;
    if (currentMACD < currentSignal && currentHist < prevHist) downScore += settings.weight_macd;

    // Pattern Logic
    if (!isDoji) {
        if (upperWick > bodySize * 2) downScore += settings.weight_pattern;
        if (lowerWick > bodySize * 2) upScore += settings.weight_pattern;
    }

    let netScore = Math.abs(upScore - downScore);
    if (upScore === downScore) {
        if (emaFast >= emaSlow) upScore += settings.weight_ema;
        else downScore += settings.weight_ema;
        netScore = Math.abs(upScore - downScore);
    }

    let pred = upScore > downScore ? "UP" : "DOWN";
    let conf = 50 + (netScore * 1.5);
    if (rvol >= settings.rvol_threshold) conf += 2.5;

    // Consecutive Candle Penalties
    let consecutiveCandles = 0;
    let lastColor = null;
    for(let i = 1; i <= 8; i++) {
        const c = candles[candles.length - 1 - i];
        const color = c[4] > c[1] ? 'green' : 'red';
        if (lastColor === null) { lastColor = color; consecutiveCandles = 1; } 
        else if (color === lastColor) consecutiveCandles++;
        else break;
    }

    if (consecutiveCandles >= 5) conf -= settings.penalty_5_candles;
    else if (consecutiveCandles === 4) conf -= settings.penalty_4_candles;
    else if (consecutiveCandles === 3) conf += settings.penalty_3_candles;
    if (isDoji) conf -= 3.0;

    let reqConf = atrPercentage > settings.volatility_threshold ? settings.high_volatility_confidence : settings.base_confidence;
    
    if (conf < reqConf) pred = "SKIP";

    return { pred, conf };
}

async function runBacktest() {
    console.log("📥 Fetching 10,000 historical 5m candles from Binance...");
    const exchange = new ccxt.binance();
    let allCandles = [];
    let since = exchange.milliseconds() - (10000 * 5 * 60 * 1000);

    while (allCandles.length < 10000) {
        const batch = await exchange.fetchOHLCV('BNB/USDT', '5m', since, 1000);
        if (batch.length === 0) break;
        allCandles = allCandles.concat(batch);
        since = batch[batch.length - 1][0] + 1;
        console.log(`...fetched ${allCandles.length} candles`);
    }

    // Split 70% In-Sample, 30% Out-Of-Sample
    const splitIndex = Math.floor(allCandles.length * 0.7);
    const inSample = allCandles.slice(0, splitIndex);
    const outOfSample = allCandles.slice(splitIndex);

    const testPhase = (dataArray, phaseName) => {
        let wins = 0, losses = 0, skips = 0;
        
        for (let i = 50; i < dataArray.length - 1; i++) {
            const historicalSlice = dataArray.slice(i - 50, i);
            const { pred } = simulatePrediction(historicalSlice);
            
            // The actual result is determined by the next candle (simulate PancakeSwap close)
            const lockPrice = dataArray[i-1][4]; 
            const closePrice = dataArray[i][4];
            const actualResult = closePrice > lockPrice ? "UP" : "DOWN";

            if (pred === "SKIP") {
                skips++;
            } else if (pred === actualResult) {
                wins++;
            } else {
                losses++;
            }
        }

        const winRate = wins + losses > 0 ? ((wins / (wins + losses)) * 100).toFixed(2) : 0;
        console.log(`\n📊 --- ${phaseName} RESULTS ---`);
        console.log(`Wins: ${wins} | Losses: ${losses} | Skips: ${skips}`);
        console.log(`Strict Win Rate: ${winRate}%`);
    };

    testPhase(inSample, "IN-SAMPLE (Sandbox Phase)");
    testPhase(outOfSample, "OUT-OF-SAMPLE (Lie Detector Phase)");
}

runBacktest().catch(console.error);
