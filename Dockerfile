FROM nogil/python:latest
WORKDIR /neo4chain
COPY . /neo4chain/
RUN python -m pip install -r requirements.txt

ENTRYPOINT ["python"]
