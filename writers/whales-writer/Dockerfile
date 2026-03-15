FROM python:3.11
WORKDIR /app
COPY common /common
RUN pip install /common
COPY writers/whales-writer /app
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "whales_writer.py" ]