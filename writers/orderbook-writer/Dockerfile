FROM python:3.11
WORKDIR /app
COPY common /common
RUN pip install /common
COPY writers/orderbook-writer /app
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "orderbook_writer.py" ]