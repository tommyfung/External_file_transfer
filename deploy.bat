@echo off
docker buildx build --platform linux/amd64 -t gcr.io/spheric-crowbar-433702-g7/my-python-app .
docker push gcr.io/spheric-crowbar-433702-g7/my-python-app
gcloud run deploy my-python-app --image gcr.io/spheric-crowbar-433702-g7/my-python-app --platform managed --region asia-east2 --allow-unauthenticated
