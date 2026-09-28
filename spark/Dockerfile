FROM apache/spark:3.5.0

USER root

# Copy requirements and install MLflow, LightGBM, and PyArrow for vectorized streaming inference
COPY requirements.txt /opt/spark/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/spark/requirements.txt

# Return to spark working directory
WORKDIR /opt/spark
