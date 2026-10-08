# Spark 4.2.0 (official image, Scala 2.13, Java 21, PySpark included)
ARG SPARK_IMAGE=apache/spark:4.2.0-python3
FROM ${SPARK_IMAGE}

USER root

# The upstream image creates the `spark` user with a non-existent home, which
# Jupyter, Ivy (spark.jars.packages) and the Spark event log all need. gosu
# resets HOME from /etc/passwd when it drops privileges, so the home has to be
# fixed on the user itself rather than only through ENV.
RUN mkdir -p /home/spark && chown spark:spark /home/spark \
 && usermod --home /home/spark --shell /bin/bash spark

# JupyterLab lives in a venv so it can never collide with the system Python.
# Arrow-optimised Python UDFs / PySpark IPC are on by default in Spark 4.2.
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-venv \
 && rm -rf /var/lib/apt/lists/* \
 && python3 -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/venv/bin/pip install --no-cache-dir "jupyterlab==4.6.4"

ENV PATH="/opt/venv/bin:/opt/spark/bin:${PATH}" \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python \
    HOME=/home/spark \
    JUPYTER_RUNTIME_DIR=/home/spark/.local/share/jupyter/runtime

# spark-submit puts PySpark on PYTHONPATH for itself, but a Jupyter kernel is
# started directly and would fail on `import pyspark`. A .pth file makes every
# interpreter in the venv see $SPARK_HOME/python and the py4j zip, whichever
# py4j version the Spark release shipped.
RUN PYTHON_SITE="$(ls -d /opt/venv/lib/python3.*/site-packages)" \
 && printf '%s\n' \
      "import sys, glob; sys.path[:0] = ['/opt/spark/python'] + sorted(glob.glob('/opt/spark/python/lib/py4j-*.zip'))" \
      > "${PYTHON_SITE}/spark-python.pth" \
 && /opt/venv/bin/python -c "import pyspark.sql; print('pyspark import OK')"

COPY spark-defaults.conf /opt/spark/conf/
COPY ivysettings.xml /opt/spark/conf/
COPY start.sh /opt/start.sh
RUN chmod +x /opt/start.sh

WORKDIR /opt/spark/work-dir

# Runs as root on purpose: start.sh needs to fix up the ownership of the
# bind-mounted notebook directory before dropping privileges with gosu.
USER root
