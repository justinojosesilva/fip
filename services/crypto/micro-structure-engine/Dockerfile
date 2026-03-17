FROM python:3.11
WORKDIR /app
COPY common /common
RUN pip install /common
COPY services/crypto/micro-structure-engine /app
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "micro_structure_engine.py" ]