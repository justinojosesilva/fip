FROM python:3.11
WORKDIR /app
COPY common /common
RUN pip install /common
COPY writers/trades-writer /app
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "trades_writer.py" ]