FROM python:3
WORKDIR /neo4chain
COPY . /neo4chain/
RUN python -m pip install --no-cache-dir  -r requirements.txt

ENTRYPOINT ["python"]
