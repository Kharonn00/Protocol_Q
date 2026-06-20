import math

def norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def get_binary_option_price(S, K, t_remaining_seconds, sigma_annual):
    t_remaining_years = t_remaining_seconds / (365 * 24 * 3600)
    val = sigma_annual * math.sqrt(t_remaining_years)
    if val <= 0.0:
        return 1.0 if S > K else 0.0
    d2 = (math.log(S / K) - 0.5 * (sigma_annual ** 2) * t_remaining_years) / val
    return norm_cdf(d2)

# Assume price is 0.0830
S = 0.0830
sigma = 0.40 # 40% annual volatility

print("Seconds left: 180 (3 minutes)")
for diff in [-0.0005, -0.0002, -0.0001, -0.00005, 0.0, 0.00005, 0.0001, 0.0002, 0.0005]:
    K = S - diff
    p = get_binary_option_price(S, K, 180, sigma)
    print(f"Strike K: {K:.6f} | Price diff: {diff:+.6f} | YES option price: {p:.4f}")
