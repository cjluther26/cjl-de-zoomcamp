## run `docker run hello-world` in Terminal to check if Docker is installed correctly

# Installing pandas (getting to bash, to be able to install, need to overwrite the entrypoint)
# `docker run -it --entrypoint=bash python:3.9`
#### then, type `python` to execute python in the container (hit CTRL+D to exit...note that if you start the container again from this image, it won't have pandas!)

FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2
RUN pip install pyarrow

WORKDIR /app
COPY ingest_data.py pipeline.py

# ENTRYPOINT [ "bash" ]
ENTRYPOINT [ "python", "pipeline.py" ]

