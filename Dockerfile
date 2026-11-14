FROM apache/spark:3.5.3

COPY service.py /opt/spark-app/service.py

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=4)" || exit 1

CMD ["python3", "/opt/spark-app/service.py"]
