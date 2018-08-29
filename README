# Ingest service worker

Just some experimentation with creating ingest service. Needs ECTD MINIO and Elastic running somewhere on the network
the containers can scale up and down and distribute jobs evenly between them. uses ECTD for job storage and URL caching

currently only RSS/News feeds are supported.
Hopefully expand to cover:
- twitter
- reddit
- weather
- stocks
- hackernews

Main Aim to to take over the workd :p


docker stack init //if first time creating a service
docker-compose build
docker stack deploy -c docker-compose.yml ingest_service