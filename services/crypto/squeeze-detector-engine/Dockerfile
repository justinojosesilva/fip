FROM python:3.11
WORKDIR /app
COPY common /common
RUN pip install /common
COPY services/crypto/squeeze-detector-engine /app
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "squeeze_detector_engine.py" ]