use pyo3::prelude::*;

#[pyclass]
pub struct FastIndicators {
    alpha: f64,
    mean: f64,
    variance: f64,
    count: usize,
    
    // RSI smoothing
    avg_gain: f64,
    avg_loss: f64,
    period: usize,
    last_price: f64,

    // Multi-timeframe volatility (slow EMA)
    slow_mean: f64,
    slow_alpha: f64,
}

#[pymethods]
impl FastIndicators {
    #[new]
    pub fn new(period: usize, alpha: f64) -> Self {
        let period = if period == 0 { 1 } else { period };
        let clamped_alpha = alpha.clamp(0.0, 1.0);
        FastIndicators {
            alpha: clamped_alpha,
            mean: 0.0,
            variance: 0.0,
            count: 0,
            
            avg_gain: 0.0,
            avg_loss: 0.0,
            period,
            last_price: 0.0,

            slow_mean: 0.0,
            slow_alpha: clamped_alpha / 5.0,
        }
    }

    pub fn add_price(&mut self, price: f64) {
        if !price.is_finite() { return; }

        self.count += 1;

        if self.mean == 0.0 {
            self.mean = price;
            self.variance = 0.0;
        } else {
            let delta = price - self.mean;
            self.mean += self.alpha * delta;
            self.variance = (1.0 - self.alpha) * (self.variance + self.alpha * delta * delta);
        }

        // Slow EMA update
        let slow_delta = price - self.slow_mean;
        if self.slow_mean == 0.0 {
            self.slow_mean = price;
        } else {
            self.slow_mean += self.slow_alpha * slow_delta;
        }

        // RSI Logic
        if self.count > 1 {
            let diff = price - self.last_price;
            let current_gain = if diff > 0.0 { diff } else { 0.0 };
            let current_loss = if diff < 0.0 { diff.abs() } else { 0.0 };

            if self.count <= self.period + 1 {
                self.avg_gain += current_gain;
                self.avg_loss += current_loss;
                if self.count == self.period + 1 {
                    self.avg_gain /= self.period as f64;
                    self.avg_loss /= self.period as f64;
                }
            } else {
                let p = self.period as f64;
                self.avg_gain = (self.avg_gain * (p - 1.0) + current_gain) / p;
                self.avg_loss = (self.avg_loss * (p - 1.0) + current_loss) / p;
            }
        }
        self.last_price = price;
    }

    /// Volume-weighted price update. Uses logarithmic volume scaling
    /// to weight the EMA alpha, capped at 3x to prevent overreaction.
    pub fn add_price_with_volume(&mut self, price: f64, volume: f64) {
        if !price.is_finite() || !volume.is_finite() || volume <= 0.0 { return; }

        let vol_weight = (1.0 + volume).ln().min(3.0);
        let effective_alpha = (self.alpha * vol_weight).min(0.99);

        self.count += 1;

        if self.mean == 0.0 {
            self.mean = price;
            self.variance = 0.0;
        } else {
            let delta = price - self.mean;
            self.mean += effective_alpha * delta;
            self.variance = (1.0 - effective_alpha) * (self.variance + effective_alpha * delta * delta);
        }

        // Slow EMA update (volume-weighted)
        let effective_slow_alpha = (self.slow_alpha * vol_weight).min(0.99);
        let slow_delta = price - self.slow_mean;
        if self.slow_mean == 0.0 {
            self.slow_mean = price;
        } else {
            self.slow_mean += effective_slow_alpha * slow_delta;
        }

        // RSI Logic
        if self.count > 1 {
            let diff = price - self.last_price;
            let current_gain = if diff > 0.0 { diff } else { 0.0 };
            let current_loss = if diff < 0.0 { diff.abs() } else { 0.0 };

            if self.count <= self.period + 1 {
                self.avg_gain += current_gain;
                self.avg_loss += current_loss;
                if self.count == self.period + 1 {
                    self.avg_gain /= self.period as f64;
                    self.avg_loss /= self.period as f64;
                }
            } else {
                let p = self.period as f64;
                self.avg_gain = (self.avg_gain * (p - 1.0) + current_gain) / p;
                self.avg_loss = (self.avg_loss * (p - 1.0) + current_loss) / p;
            }
        }
        self.last_price = price;
    }

    /// Returns the trend alignment signal: fast EMA minus slow EMA.
    /// Positive values indicate upward momentum, negative values indicate downward.
    pub fn get_trend_alignment(&self) -> f64 {
        self.mean - self.slow_mean
    }

    pub fn get_rsi(&self) -> f64 {
        if self.count < self.period + 1 {
            return 50.0; 
        }
        if self.avg_loss == 0.0 { return 100.0; }
        
        let rs = self.avg_gain / self.avg_loss;
        let rsi = 100.0 - (100.0 / (1.0 + rs));
        
        if rsi.is_nan() { 50.0 } else { rsi }
    }

    pub fn get_bollinger_bands(&self) -> (f64, f64, f64) {
        let std_dev = if self.variance > 0.0 { self.variance.sqrt() } else { 0.0 };
        (self.mean, self.mean + (2.0 * std_dev), self.mean - (2.0 * std_dev))
    }

    pub fn get_z_score(&self) -> f64 {
        if self.count < 2 || self.variance <= 0.0 { return 0.0; }
        let std_dev = self.variance.sqrt();
        (self.last_price - self.mean) / std_dev
    }
}

/// O(1) 60-second rolling index tracker for CF Benchmark simulation
#[pyclass]
pub struct IndexLagTracker {
    ticks: std::collections::VecDeque<(f64, f64)>, // (timestamp, price)
    sum_price: f64,
    window_sec: f64,
}

#[pymethods]
impl IndexLagTracker {
    #[new]
    pub fn new(window_sec: Option<f64>) -> Self {
        IndexLagTracker {
            ticks: std::collections::VecDeque::with_capacity(1024),
            sum_price: 0.0,
            window_sec: window_sec.unwrap_or(60.0),
        }
    }

    pub fn add_tick(&mut self, timestamp: f64, price: f64) {
        if !price.is_finite() || !timestamp.is_finite() || price <= 0.0 { return; }

        // SEV-3: Reject out-of-order timestamps to prevent queue traps
        if let Some(&(last_ts, _)) = self.ticks.back() {
            if timestamp < last_ts { return; }
        }

        self.ticks.push_back((timestamp, price));
        self.sum_price += price;

        // SEV-1: Hard capacity cap to enforce strict O(1) space invariant under tick floods
        while self.ticks.len() > 5000 {
            if let Some((_, old_price)) = self.ticks.pop_front() {
                self.sum_price -= old_price;
            }
        }

        let cutoff = timestamp - self.window_sec;
        while let Some(&(ts, old_price)) = self.ticks.front() {
            if ts < cutoff {
                self.sum_price -= old_price;
                self.ticks.pop_front();
            } else {
                break;
            }
        }

        if self.ticks.is_empty() {
            self.sum_price = 0.0;
        } else if self.ticks.len() % 128 == 0 {
            self.sum_price = self.ticks.iter().map(|&(_, p)| p).sum();
        }
    }

    pub fn get_average(&self) -> f64 {
        if self.ticks.is_empty() { return 0.0; }
        self.sum_price / (self.ticks.len() as f64)
    }

    /// Returns fractional divergence: (spot_price - 60s_avg) / 60s_avg
    pub fn get_divergence(&self, current_spot: f64) -> f64 {
        let avg = self.get_average();
        if avg <= 0.0 || !current_spot.is_finite() || current_spot <= 0.0 { return 0.0; }
        (current_spot - avg) / avg
    }

    pub fn len(&self) -> usize {
        self.ticks.len()
    }
}

/// O(1) 30-second Taker Order Flow Imbalance tracker (un-spoofable trade tape)
#[pyclass]
pub struct TakerOrderFlowTracker {
    trades: std::collections::VecDeque<(f64, f64, bool)>, // (timestamp, volume_notional, is_buy)
    total_buy_vol: f64,
    total_sell_vol: f64,
    window_sec: f64,
}

#[pymethods]
impl TakerOrderFlowTracker {
    #[new]
    pub fn new(window_sec: Option<f64>) -> Self {
        TakerOrderFlowTracker {
            trades: std::collections::VecDeque::with_capacity(1024),
            total_buy_vol: 0.0,
            total_sell_vol: 0.0,
            window_sec: window_sec.unwrap_or(30.0),
        }
    }

    pub fn add_trade(&mut self, timestamp: f64, volume_notional: f64, is_buy: bool) {
        if !volume_notional.is_finite() || volume_notional <= 0.0 || !timestamp.is_finite() { return; }

        // SEV-3: Reject out-of-order timestamps to prevent queue traps
        if let Some(&(last_ts, _, _)) = self.trades.back() {
            if timestamp < last_ts { return; }
        }

        self.trades.push_back((timestamp, volume_notional, is_buy));
        if is_buy {
            self.total_buy_vol += volume_notional;
        } else {
            self.total_sell_vol += volume_notional;
        }

        // SEV-1: Hard capacity cap to enforce strict O(1) space invariant under trade floods
        while self.trades.len() > 5000 {
            if let Some((_, old_vol, old_is_buy)) = self.trades.pop_front() {
                if old_is_buy {
                    self.total_buy_vol = (self.total_buy_vol - old_vol).max(0.0);
                } else {
                    self.total_sell_vol = (self.total_sell_vol - old_vol).max(0.0);
                }
            }
        }

        let cutoff = timestamp - self.window_sec;
        while let Some(&(ts, old_vol, old_is_buy)) = self.trades.front() {
            if ts < cutoff {
                if old_is_buy {
                    self.total_buy_vol = (self.total_buy_vol - old_vol).max(0.0);
                } else {
                    self.total_sell_vol = (self.total_sell_vol - old_vol).max(0.0);
                }
                self.trades.pop_front();
            } else {
                break;
            }
        }

        if self.trades.is_empty() {
            self.total_buy_vol = 0.0;
            self.total_sell_vol = 0.0;
        } else if self.trades.len() % 128 == 0 {
            self.total_buy_vol = self.trades.iter().filter(|&&(_, _, is_b)| is_b).map(|&(_, v, _)| v).sum();
            self.total_sell_vol = self.trades.iter().filter(|&&(_, _, is_b)| !is_b).map(|&(_, v, _)| v).sum();
        }
    }

    /// Returns (total_buy_vol, total_sell_vol, buy_to_sell_ratio)
    pub fn get_metrics(&self) -> (f64, f64, f64) {
        let buy = self.total_buy_vol;
        let sell = self.total_sell_vol;
        let ratio = if sell > 0.0 {
            buy / sell
        } else if buy > 0.0 {
            100.0 // Cap at 100 if no sell volume
        } else {
            1.0
        };
        (buy, sell, ratio)
    }

    pub fn len(&self) -> usize {
        self.trades.len()
    }
}

#[pymodule]
fn kalshi_bot(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<FastIndicators>()?;
    m.add_class::<IndexLagTracker>()?;
    m.add_class::<TakerOrderFlowTracker>()?;
    Ok(())
}