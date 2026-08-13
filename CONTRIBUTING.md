# Contributing to Kalshi Trading Bot

## Welcome & Overview
Welcome to the Kalshi trading bot project. This project is a high-frequency asynchronous trading engine. We use Python and Rust components. We appreciate your contributions.

## Development Setup

### Prerequisites
You need these tools:
- Python >=3.12
- Rust >=1.85
- maturin

### Step-by-step Setup

```bash
# 1. Clone the repository
git clone https://github.com/Kharonn00/Protocol_Q.git
cd Protocol_Q

# 2. Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate

# 3. Install Python dependencies
pip install -r requirements.txt maturin

# 4. Compile the Rust PyO3 extension
maturin develop --release

# 5. Verify compilation
python -m py_compile kalshi_main.py engine/*.py
cargo check
```

### Run in Simulation Mode

Run the bot without live API credentials:

```bash
BOT_ENV=simulation python kalshi_main.py
```

## Project Structure
- `engine/config.py` - Loads trading parameters from environment variables.
- `engine/models.py` - Data models, state containers, and Python indicator fallbacks.
- `engine/security.py` - SSRF protection, log sanitization, and circuit breaker.
- `engine/broker.py` - Simulation and live broker with RSA-2048 API authentication.
- `engine/strategy.py` - Main trading engine with three active strategies.
- `src/lib.rs` - Rust PyO3 native extension for O(1) technical indicators.
- `kalshi_main.py` - Application entry point.

## Code Style & Standards

### Python
- Follow PEP 8 guidelines.
- Use type hints.
- Write docstrings in ASD-STE100 Simplified Technical English.

### Rust
- Follow standard Rust formatting. Run `cargo fmt`.
- Use `cargo clippy`.

### All Comments
Use active voice. Use short sentences. Use simple words (ASD-STE100).

## Making Changes
1. Fork the repository.
2. Create a feature branch from the `main` branch.
3. Make your changes. Write clear, atomic commits.
4. Run verification:
   - `python -m py_compile kalshi_main.py engine/*.py`
   - `cargo check`
5. Submit a Pull Request. Include a clear description of your changes.

## Testing
- Run `python -m py_compile` to verify Python syntax.
- Run `cargo check` and `cargo test` for Rust code.
- Test in simulation mode before you submit changes.

## Reporting Issues
Use GitHub Issues to report problems. Include this information:
- Description of the problem.
- Steps to reproduce the problem.
- Expected behavior and actual behavior.
- Environment information.

## Security
If you find a security vulnerability, do NOT open a public issue. Email the maintainers directly.

## License
All contributions are under the MIT License.
