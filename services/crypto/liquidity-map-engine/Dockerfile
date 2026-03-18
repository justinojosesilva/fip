FROM python:3.11
WORKDIR /app
COPY common /common
RUN pip install /common
COPY services/crypto/liquidity-map-engine /app
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "liquidity_map_engine.py" ]