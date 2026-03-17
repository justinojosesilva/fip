FROM python:3.11
WORKDIR /app
COPY common /common
RUN pip install /common
COPY services/crypto/stop-hunt-engine /app
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "stop_hunt_engine.py" ]