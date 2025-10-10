FROM ruby:3.4.7-slim-trixie

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    ffmpeg

WORKDIR /app

# Copy Gemfile and install dependencies
COPY Gemfile Gemfile.lock ./
RUN bundle install

# Copy application code
COPY . .

# Expose ports
# 3000 for SSE server
# 1935 for RTMP (if needed)
EXPOSE 3000 1935
