FROM python:3.11
WORKDIR /app
COPY common /common
RUN pip install /common
COPY services/crypto/market-regime-engine /app
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "market_regime_engine.py" ]