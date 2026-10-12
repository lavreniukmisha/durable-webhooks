FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
RUN useradd --create-home relay && mkdir /data && chown relay:relay /data
COPY relay ./relay
COPY demo_receiver.py ./
USER relay
CMD ["python", "-m", "relay", "serve"]
