# telegram_arbitrage_bot
Create a telegram bot that sends arbitrage opportunities

## Project Structure
```
telegram_arbitrage_bot/
├── dataset/               # Data storage and manipulation
├── utils/                # Utility functions
├── main.py              # Main application file
├── pyproject.toml       # Project dependencies
└── README.md           # Project documentation
```

## Prerequisites
- Python 3.11 or higher
- Git
- uv (Python package installer)

## Installation

1. Clone the repository
```bash
git clone https://github.com/yourusername/telegram_arbitrage_bot.git
cd telegram_arbitrage_bot
```

2. Install uv (if not already installed)
```bash
pip install uv
```

3. Create and activate a virtual environment using uv
```bash
uv venv
# On Windows
.venv\Scripts\activate
# On Unix/MacOS
source .venv/bin/activate
```

4. Install dependencies using uv
```bash
uv pip install -r pyproject.toml
```

## Usage
Run the main script:
```bash
python main.py
```

## Dependencies
- orjson>=3.10.15
- polars

## Contributing
1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request
