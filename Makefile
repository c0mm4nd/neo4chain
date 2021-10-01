create-docker-neo4j:
	docker run \
		--name neo4j_tmp \
		-d \
		--publish=7474:7474 --publish=7687:7687 \
		--volume=${PWD}/data:/data \
		--env=NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
		--env NEO4J_AUTH=neo4j/icaneatglass \
		neo4j:enterprise