#!/usr/bin/env ruby

require 'bundler/setup'
require 'async'
require 'async/redis'
require 'json'

# Redis connection settings
# Parse Redis URL (supports redis:// and rediss:// for TLS)
redis_url = ENV.fetch('REDIS_URL', 'redis://localhost:6379/0')
redis_endpoint = Async::Redis::Endpoint.parse(redis_url)

stream_key = ENV.fetch('REDIS_STREAM_KEY', 'transcription_stream')

def send_test_message(client, stream_key, room: 'default', language: 'ja', text: nil)
  message_text = text || "Test message at #{Time.now.iso8601}"

  # XADD stream_key * field1 value1 field2 value2 ...
  result = client.call('XADD', stream_key, '*',
    'room', room,
    'language', language,
    'text', message_text,
    'timestamp', Time.now.iso8601,
    'type', 'transcription'
  )

  puts "Sent message with ID: #{result}"
  puts "  Room: #{room}, Language: #{language}"
  puts "  Text: #{message_text}"
  result
end

Async do
  Async::Redis::Client.open(redis_endpoint) do |client|
    puts "Connected to Redis"
    puts "Stream key: #{stream_key}"
    puts ""

    # Test connection
    response = client.call('PING')
    puts "Redis PING response: #{response}"
    puts ""

    # Send test messages
    puts "Sending test messages..."

    # Message 1: Default room, Japanese
    send_test_message(client, stream_key,
      room: 'default',
      language: 'ja',
      text: 'こんにちは、これはテストメッセージです。'
    )

    sleep 1

    # Message 2: Default room, English
    send_test_message(client, stream_key,
      room: 'default',
      language: 'en',
      text: 'Hello, this is a test message.'
    )

    sleep 1

    # Message 3: Conference room, Japanese
    send_test_message(client, stream_key,
      room: 'conference',
      language: 'ja',
      text: '会議室からのメッセージです。'
    )

    sleep 1

    # Message 4: Default room, Japanese (should be received by default SSE client)
    send_test_message(client, stream_key,
      room: 'default',
      language: 'ja',
      text: 'SSEクライアントに配信されるメッセージ'
    )

    puts ""
    puts "All test messages sent!"

    # Check stream info
    info = client.call('XINFO', 'STREAM', stream_key)
    puts ""
    puts "Stream info:"
    puts "  Length: #{info[1]}"
    puts "  First entry: #{info[9]}"
    puts "  Last entry: #{info[11]}"
  end
rescue => e
  puts "Error: #{e.message}"
  puts e.backtrace.first(5)
end
