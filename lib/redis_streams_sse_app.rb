require 'async'
require 'async/redis'
require 'rack'
require 'json'

class RedisStreamsSSEApp
  def initialize
    @redis_endpoint = Async::Redis.local_endpoint(
      host: ENV.fetch('REDIS_HOST', 'localhost'),
      port: ENV.fetch('REDIS_PORT', 6379).to_i,
      db: ENV.fetch('REDIS_DB', 0).to_i
    )
    @stream_key = ENV.fetch('REDIS_STREAM_KEY', 'transcription_stream')
  end

  def call(env)
    request = Rack::Request.new(env)

    case request.path
    when '/sse'
      handle_sse_stream(request)
    when '/health'
      handle_health_check
    else
      [404, {'Content-Type' => 'text/plain'}, ['Not Found']]
    end
  end

  private

  def handle_sse_stream(request)
    headers = {
      'Content-Type' => 'text/event-stream',
      'Cache-Control' => 'no-cache',
      'Connection' => 'keep-alive',
      'Access-Control-Allow-Origin' => '*',
      'Access-Control-Allow-Headers' => 'Cache-Control',
      'X-Accel-Buffering' => 'no'
    }

    # Get language from query params
    language = request.params['language'] || 'ja'
    room = request.params['room'] || 'default'

    [200, headers, RedisStreamSSE.new(@redis_endpoint, @stream_key, room, language)]
  end

  def handle_health_check
    status = {
      status: 'ok',
      timestamp: Time.now.iso8601,
      redis_status: check_redis_connection
    }
    [200, {'Content-Type' => 'application/json'}, [status.to_json]]
  end

  def check_redis_connection
    Async do
      Async::Redis::Client.open(@redis_endpoint) do |client|
        response = client.call('PING')
        response == 'PONG' ? 'connected' : 'disconnected'
      end
    end.wait
  rescue => e
    "error: #{e.message}"
  end
end

class RedisStreamSSE
  def initialize(redis_endpoint, stream_key, room, language)
    @redis_endpoint = redis_endpoint
    @stream_key = stream_key
    @room = room
    @language = language
    @last_id = '0-0'
  end

  def each
    # Send initial connection message
    yield "event: connected\n"
    yield "data: #{JSON.generate({
      message: 'Connected to Redis Streams SSE',
      stream: @stream_key,
      room: @room,
      language: @language,
      timestamp: Time.now.iso8601
    })}\n\n"

    # Start reading from Redis Streams
    redis_task = read_redis_stream { |event, data|
      yield "event: #{event}\n"
      yield "data: #{data}\n\n"
    }

    # Send heartbeat every 30 seconds
    heartbeat_task = Async do
      loop do
        sleep 30
        yield "event: heartbeat\n"
        yield "data: #{JSON.generate({ timestamp: Time.now.iso8601 })}\n\n"
      end
    end

    # Wait for tasks
    redis_task.wait
    heartbeat_task.stop

  rescue => e
    yield "event: error\n"
    yield "data: #{JSON.generate({ error: e.message })}\n\n"
  ensure
    redis_task&.stop
    heartbeat_task&.stop
  end

  private

  def read_redis_stream
    Async do
      begin
        Async::Redis::Client.open(@redis_endpoint) do |client|
          puts "[RedisStreamSSE] Connected to Redis, reading from stream: #{@stream_key}"

          loop do
            begin
              # XREAD with block timeout of 5 seconds
              # Format: XREAD BLOCK 5000 STREAMS stream_key last_id
              result = client.call('XREAD', 'BLOCK', '5000', 'STREAMS', @stream_key, @last_id)

              if result && result.is_a?(Array) && !result.empty?
                # Parse XREAD response
                # Format: [[stream_name, [[id, [field1, value1, field2, value2, ...]], ...]]]
                stream_data = result[0]
                stream_name = stream_data[0]
                entries = stream_data[1]

                entries.each do |entry|
                  entry_id = entry[0]
                  fields = entry[1]

                  # Convert field array to hash
                  data = {}
                  fields.each_slice(2) do |key, value|
                    data[key] = value.force_encoding('UTF-8')
                  end

                  # Filter by room and language if specified in the data
                  if should_send_message?(data)
                    message = {
                      id: entry_id,
                      stream: stream_name,
                      data: data,
                      timestamp: Time.now.iso8601
                    }

                    yield 'message', JSON.generate(message)
                    puts "[RedisStreamSSE] Sent message: #{entry_id}"
                  end

                  @last_id = entry_id
                end
              end

            rescue => e
              puts "[RedisStreamSSE] Error reading stream: #{e.message}"
              yield 'error', JSON.generate({ error: e.message })
              sleep 1
            end
          end
        end
      rescue => e
        puts "[RedisStreamSSE] Redis connection error: #{e.message}"
        yield 'error', JSON.generate({ error: "Redis connection failed: #{e.message}" })

        # Retry connection after 5 seconds
        sleep 5
        retry
      end
    end
  end

  def should_send_message?(data)
    # Check if message matches room and language filters
    message_room = data['room'] || 'default'
    message_language = data['language'] || 'ja'

    return true if @room == 'all' || @language == 'all'
    return message_room == @room && message_language == @language
  end
end
