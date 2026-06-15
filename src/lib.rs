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

            if self.count <= self.period + 1 && self.avg_gain == 0.0 && self.avg_loss == 0.0 {
                self.avg_gain = current_gain;
                self.avg_loss = current_loss;
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

            if self.count <= self.period + 1 && self.avg_gain == 0.0 && self.avg_loss == 0.0 {
                self.avg_gain = current_gain;
                self.avg_loss = current_loss;
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

#[pymodule]
fn kalshi_bot(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<FastIndicators>()?;
    Ok(())
}