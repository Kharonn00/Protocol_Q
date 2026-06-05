use pyo3::prelude::*;
use std::collections::VecDeque;

#[pyclass]
pub struct FastIndicators {
    period: usize,
    prices: VecDeque<f64>,
    avg_gain: f64,
    avg_loss: f64,
}

#[pymethods]
impl FastIndicators {
    #[new]
    pub fn new(period: usize) -> Self {
        FastIndicators {
            period,
            prices: VecDeque::with_capacity(period + 1), 
            avg_gain: 0.0,
            avg_loss: 0.0,
        }
    }

    pub fn add_price(&mut self, price: f64) {
        if !price.is_finite() { 
            return; 
        }

        self.prices.push_back(price);
        if self.prices.len() > self.period + 1 {
            self.prices.pop_front();
        }

        let len = self.prices.len();
        if len > 1 {
            let diff = self.prices[len - 1] - self.prices[len - 2];
            let current_gain = if diff > 0.0 { diff } else { 0.0 };
            let current_loss = if diff < 0.0 { diff.abs() } else { 0.0 };

            if len <= self.period + 1 && self.avg_gain == 0.0 && self.avg_loss == 0.0 {
                // Initialize SMA seed
                self.avg_gain = current_gain;
                self.avg_loss = current_loss;
            } else {
                // Wilder's Exponential Smoothing
                let p = self.period as f64;
                self.avg_gain = (self.avg_gain * (p - 1.0) + current_gain) / p;
                self.avg_loss = (self.avg_loss * (p - 1.0) + current_loss) / p;
            }
        }
    }

    pub fn get_rsi(&self) -> f64 {
        if self.prices.len() < self.period + 1 {
            return 50.0; 
        }
        if self.avg_loss == 0.0 { return 100.0; }
        
        let rs = self.avg_gain / self.avg_loss;
        let rsi = 100.0 - (100.0 / (1.0 + rs));
        
        if rsi.is_nan() { 50.0 } else { rsi }
    }

    pub fn get_bollinger_bands(&self) -> (f64, f64, f64) {
        if self.prices.is_empty() { return (0.0, 0.0, 0.0); }
        
        let len = self.prices.len() as f64;
        let mean: f64 = self.prices.iter().sum::<f64>() / len;
        
        // M-3 FIX: Bessel's Correction for Sample Variance
        let variance: f64 = if len > 1.0 {
            self.prices.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / (len - 1.0)
        } else {
            0.0
        };
        let std_dev = variance.sqrt();
        
        if mean.is_nan() || std_dev.is_nan() {
            return (0.0, 0.0, 0.0);
        }
        
        (mean, mean + (2.0 * std_dev), mean - (2.0 * std_dev))
    }
}

#[pymodule]
fn kalshi_bot(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<FastIndicators>()?;
    Ok(())
}