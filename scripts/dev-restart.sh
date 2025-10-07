#!/bin/bash

# Quick development restart script for Trino Demo
# This script helps with the development workflow

echo "🔄 Development Restart Script for Trino Demo"
echo "============================================="

# Navigate to project root (scripts folder is one level down)
cd "$(dirname "$0")/.."

# Check if we need to rebuild the image (if Dockerfile or requirements.txt changed)
if [ "$1" = "--rebuild" ] || [ "$1" = "-r" ]; then
    echo "🏗️  Rebuilding shiny-app image..."
    cd shiny-app
    docker build -t trino-shiny-app:latest .
    cd ..
    echo "✅ Image rebuilt!"
fi

# Force recreate the shiny-app container to ensure volume mounts pick up all changes
echo "🔄 Recreating shiny-app container to pick up all Python module changes..."
docker-compose stop shiny-app
docker-compose rm -f shiny-app
docker-compose up -d shiny-app

# Wait a moment for the container to start
echo "⏳ Waiting for container to start..."
sleep 3

# Validate that volume mounts are working
echo "🔍 Validating volume mounts..."
if docker-compose exec -T shiny-app test -f /app/app.py; then
    echo "✅ Volume mounts verified - app.py is accessible"
else
    echo "❌ Warning: app.py not found in container"
fi

if docker-compose exec -T shiny-app test -d /app/shared; then
    echo "✅ Volume mounts verified - shared/ directory is accessible"
else
    echo "❌ Warning: shared/ directory not found in container"
fi

# Show the logs
echo "📋 Recent logs:"
docker-compose logs --tail=8 shiny-app

echo ""
echo "🚀 App should be available at: http://localhost:8000"
echo "📝 Container recreated - all Python module changes should be picked up!"
echo ""
echo "Usage:"
echo "  ./scripts/dev-restart.sh          # Recreate container to pick up all changes" 
echo "  ./scripts/dev-restart.sh --rebuild # Rebuild image and recreate container"