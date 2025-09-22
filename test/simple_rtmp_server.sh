#!/bin/bash

# Simple RTMP server using FFmpeg
# Listens on rtmp://localhost:1935/live

echo "Starting simple RTMP server on port 1935..."
echo "Stream URL: rtmp://localhost:1935/live"
echo "Press Ctrl+C to stop"
echo ""

# Start FFmpeg as RTMP server
ffmpeg -listen 1 -i rtmp://localhost:1935/live \
  -c copy -f flv - | \
  ffmpeg -i - \
    -f s16le -acodec pcm_s16le -ar 16000 -ac 1 \
    -f null /dev/null 2>&1 | \
  grep -E "time=|Stream"
