require 'async'
require 'async/redis'
require 'rack'
require 'json'
require 'logger'
require 'securerandom'
require 'concurrent'
require 'sentry-ruby'

class RedisStreamsSSEApp
  def initialize(logger: nil)
    @redis_endpoint = Async::Redis.local_endpoint(
      host: ENV.fetch('REDIS_HOST', 'localhost'),
      port: ENV.fetch('REDIS_PORT', 6379).to_i,
      db: ENV.fetch('REDIS_DB', 0).to_i
    )
    @stream_key = ENV.fetch('REDIS_STREAM_KEY', 'transcription_stream')
    @connected_clients = Concurrent::Hash.new
    @logger = logger || Logger.new(STDOUT).tap do |log|
      log.formatter = proc do |severity, datetime, progname, msg|
        "[#{datetime.strftime('%Y-%m-%d %H:%M:%S')}] [RedisStreamsSSEApp] #{severity}: #{msg}\n"
      end
    end

    # Start monitoring thread for client connections
    start_connection_monitor
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

    # Set Sentry context for this request
    Sentry.configure_scope do |scope|
      scope.set_context('sse_stream', {
        room: room,
        language: language,
        path: request.path
      })
    end

    # Create SSE stream and track it
    client_id = SecureRandom.uuid
    sse_stream = RedisStreamSSE.new(@redis_endpoint, @stream_key, room, language,
                                     logger: @logger,
                                     client_id: client_id,
                                     on_close: -> { @connected_clients.delete(client_id) })

    # Track this client
    @connected_clients[client_id] = {
      connected_at: Time.now,
      room: room,
      language: language,
      remote_ip: request.ip
    }

    @logger.info "New SSE client connected: #{client_id} from #{request.ip} (room: #{room}, language: #{language})"

    [200, headers, sse_stream]
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
    Sentry.capture_exception(e)
    "error: #{e.message}"
  end

  private

  def start_connection_monitor
    Thread.new do
      loop do
        sleep(30)  # Log every 30 seconds

        active_count = @connected_clients.size
        if active_count > 0
          # Group clients by room and language
          room_stats = @connected_clients.values.group_by { |c| "#{c[:room]}/#{c[:language]}" }
                                                 .transform_values(&:count)

          # Format room statistics as a single line
          room_details = room_stats.map { |room_lang, count| "#{room_lang}: #{count}" }.join(', ')
          @logger.info "Connected clients: #{active_count} total (#{room_details})"
        else
          @logger.debug "No active SSE connections" if ENV['DEBUG']
        end
      rescue => e
        @logger.error "Connection monitor error: #{e.message}"
      end
    end
  end
end

class RedisStreamSSE
  def initialize(redis_endpoint, stream_key, room, language, logger: nil, client_id: nil, on_close: nil)
    @redis_endpoint = redis_endpoint
    @stream_key = stream_key
    @room = room
    @language = language
    @client_id = client_id || SecureRandom.uuid
    @on_close = on_close
    # Use '$' to start reading only new messages from connection time
    @last_id = '$'
    @logger = logger || Logger.new(STDOUT).tap do |log|
      log.formatter = proc do |severity, datetime, progname, msg|
        "[#{datetime.strftime('%Y-%m-%d %H:%M:%S')}] [RedisStreamSSE] #{severity}: #{msg}\n"
      end
    end
  end

  def each
    # Send initial connection message
    yield "event: connected\n"
    yield "data: #{JSON.generate({
      message: 'Connected to Redis Streams SSE',
      stream: @stream_key,
      room: @room,
      language: @language,
      client_id: @client_id,
      timestamp: Time.now.iso8601
    })}\n\n"

    @logger.debug "SSE stream started for client #{@client_id}" if ENV['DEBUG']

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
    # Report to Sentry with SSE context
    Sentry.capture_exception(e) do |scope|
      scope.set_context('sse', {
        room: @room,
        language: @language,
        last_id: @last_id
      })
    end

    yield "event: error\n"
    yield "data: #{JSON.generate({ error: e.message })}\n\n"
  ensure
    redis_task&.stop
    heartbeat_task&.stop

    # Call cleanup callback
    @on_close&.call
    @logger.info "SSE client disconnected: #{@client_id}"
  end

  private

  def read_redis_stream
    Async do
      begin
        Async::Redis::Client.open(@redis_endpoint) do |client|
          @logger.info "Connected to Redis, reading new messages from stream: #{@stream_key} (starting from: #{@last_id})"

          loop do
            begin
              # XREAD with block timeout of 5 seconds
              # Format: XREAD BLOCK 5000 STREAMS stream_key last_id
              # Using '$' initially means we only get messages that arrive after connection
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
                    @logger.debug "Sent message: #{entry_id}" if ENV['DEBUG']
                  end

                  @last_id = entry_id
                end
              end

            rescue => e
              @logger.error "Error reading stream: #{e.message}"

              # Report to Sentry
              Sentry.capture_exception(e) do |scope|
                scope.set_tag('component', 'redis_stream_reader')
                scope.set_context('stream', {
                  stream_key: @stream_key,
                  last_id: @last_id
                })
              end

              yield 'error', JSON.generate({ error: e.message })
              sleep 1
            end
          end
        end
      rescue => e
        @logger.error "Redis connection error: #{e.message}"

        # Report to Sentry
        Sentry.capture_exception(e) do |scope|
          scope.set_tag('component', 'redis_connection')
          scope.set_context('redis', {
            endpoint: @redis_endpoint.to_s
          })
        end

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
