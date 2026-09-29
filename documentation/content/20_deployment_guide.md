# 20. Docker Deployment & Orchestration Guide

### Container Cluster Configuration
The entire Big Data ecosystem runs via `docker/docker-compose.yml`:
```powershell
# Launch all 10 containers in detached mode
docker-compose -f docker/docker-compose.yml up -d

# Verify container health
docker ps

# Inspect logs for specific services
docker logs -f kafka
docker logs -f hive-server
docker logs -f spark-master
```
