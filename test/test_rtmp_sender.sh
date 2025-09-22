#!/bin/bash

# Test script to send audio to RTMP server
# Usage: ./test_rtmp_sender.sh [rtmp_url]

RTMP_URL=${1:-rtmp://localhost:1935/live}

echo "="
echo "RTMP Test Audio Sender"
echo "="
echo "Sending test audio to: $RTMP_URL"
echo "Press Ctrl+C to stop"
echo ""

# Option 1: Send microphone input (if available)
if [ -f /dev/dsp ] || [ -f /dev/audio ]; then
  echo "Using microphone input..."
  ffmpeg -f alsa -i default \
    -acodec libmp3lame -b:a 128k \
    -f flv "$RTMP_URL"

# Option 2: Generate test audio (sine wave)
else
  echo "Generating test audio (440Hz sine wave)..."
  ffmpeg -re -f lavfi -i "sine=frequency=440:duration=0" \
    -f lavfi -i "sine=frequency=880:duration=0" \
    -filter_complex "[0:a][1:a]amerge=inputs=2,pan=stereo|c0<c0+c2|c1<c1+c3[a]" \
    -map "[a]" \
    -acodec libmp3lame -b:a 128k \
    -f flv "$RTMP_URL"
fi
