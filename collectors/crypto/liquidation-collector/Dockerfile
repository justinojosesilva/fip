FROM python:3.11
WORKDIR /app
COPY common /common
RUN pip install /common
COPY collectors/crypto/liquidation-collector /app/
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "liquidation_collector.py" ]