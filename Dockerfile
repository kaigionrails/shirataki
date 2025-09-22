FROM ruby:3.3.7-bookworm

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    openssl \
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
