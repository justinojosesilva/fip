FROM python:3.11
WORKDIR /app
COPY common /common
RUN pip install /common
COPY services/crypto/whale-tracker /app
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "whale_tracker.py" ]