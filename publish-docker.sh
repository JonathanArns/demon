#!/bin/bash

# Check if the current directory is a git repository
if [ ! -d ".git" ]; then
  echo "This is not a git repository. Please run this script in the root of a git repository."
  exit 1
fi

# Get the current git commit hash (first 7 characters)
commit_hash=$(git rev-parse --short HEAD)

# Build the Docker image and tag it with 'latest' and the commit hash
docker build -t jonathanarns/semi-ser:latest -t jonathanarns/semi-ser:$commit_hash .

# Check if the docker build was successful
if [ $? -eq 0 ]; then
  echo "Docker image built and tagged successfully."

  # Push the Docker images to Docker Hub
  docker push jonathanarns/semi-ser:latest
  docker push jonathanarns/semi-ser:$commit_hash

  # Check if the docker push was successful
  if [ $? -eq 0 ]; then
    echo "Docker images pushed to Docker Hub successfully."
  else
    echo "Failed to push Docker images to Docker Hub."
    exit 1
  fi
else
  echo "Docker build failed."
  exit 1
fi
