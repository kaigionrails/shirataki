require 'async'
require 'async/redis'
require 'json'
require 'logger'
require_relative 'ffmpeg_audio_stream'
require_relative 'transcribe_client'

class RtmpTranscribeService
  def initialize(rtmp_url: nil, room: 'default', test_mode: false, logger: nil)
    @rtmp_url = rtmp_url || ENV.fetch('RTMP_URL', 'rtmp://localhost:1935/live')
    @room = room
    @test_mode = test_mode
    @audio_stream = nil
    @transcribe_client = nil
    @redis_endpoint = Async::Redis.local_endpoint(
      host: ENV.fetch('REDIS_HOST', 'localhost'),
      port: ENV.fetch('REDIS_PORT', 6379).to_i,
      db: ENV.fetch('REDIS_DB', 0).to_i
    )
    @stream_key = ENV.fetch('REDIS_STREAM_KEY', 'transcription_stream')
    @running = false
    @redis_task = nil
    @logger = logger || Logger.new(STDOUT).tap do |log|
      log.formatter = proc do |severity, datetime, progname, msg|
        "[#{datetime.strftime('%Y-%m-%d %H:%M:%S')}] [RtmpTranscribeService] #{severity}: #{msg}\n"
      end
    end
  end

  def start
    return if @running

    @running = true
    @logger.info "Starting service..."
    @logger.info "  RTMP URL: #{@rtmp_url}"
    @logger.info "  Room: #{@room}"
    @logger.info "  Redis Stream: #{@stream_key}"

    begin
      # Start FFmpeg audio stream (pass logger to child component)
      @audio_stream = FFmpegAudioStream.new(rtmp_url: @rtmp_url, logger: @logger)
      @audio_stream.start

      # Wait for audio buffer to have sufficient data before starting Transcribe
      @logger.info "Waiting for audio data to be available..."
      wait_count = 0
      while @audio_stream.buffer.size < 6400 && wait_count < 30  # Wait up to 30 seconds for at least 200ms of audio
        sleep(1)
        wait_count += 1
        if wait_count % 5 == 0
          @logger.info "Waiting for audio data... (buffer size: #{@audio_stream.buffer.size})"
        end
      end

      if @audio_stream.buffer.size < 6400
        @logger.warn "Starting with insufficient audio buffer (size: #{@audio_stream.buffer.size})"
      else
        @logger.info "Audio buffer ready (size: #{@audio_stream.buffer.size})"
      end

      # Start Transcribe client
      @transcribe_client = TranscribeClient.new(logger: @logger)
      @transcribe_client.start(@audio_stream)

      # Start Redis publishing task
      start_redis_publisher

      # Monitor loop
      monitor_services

    rescue => e
      @logger.error "Error: #{e.message}"
      @logger.error e.backtrace.first(5).join("\n") if e.backtrace
      stop
    end
  end

  def stop
    return unless @running

    @logger.info "Stopping service..."
    @running = false

    # Stop components
    @redis_task&.stop
    @transcribe_client&.stop
    @audio_stream&.stop

    @logger.info "Service stopped"
  end

  private

  def start_redis_publisher
    @redis_task = Async do
      last_processed_index = 0

      while @running
        begin
          # Check for new transcription results
          results = @transcribe_client.results

          if results.size > last_processed_index
            # Process new results
            new_results = results[last_processed_index..-1]

            Async::Redis::Client.open(@redis_endpoint) do |client|
              new_results.each do |result|
                publish_to_redis(client, result)
              end
            end

            last_processed_index = results.size
          end

          # Check every 100ms
          sleep(0.1)

        rescue => e
          @logger.error "Redis publisher error: #{e.message}"
          sleep(1)
        end
      end
    end
  end

  def publish_to_redis(client, result)
    # Prepare message for Redis Stream
    message_data = {
      'room' => @room,
      'language' => result[:language] || 'ja',
      'text' => result[:text],
      'is_final' => result[:is_final].to_s,
      'timestamp' => result[:timestamp],
      'type' => 'transcription'
    }

    # Add confidence if available
    if result[:confidence]
      message_data['confidence'] = result[:confidence].to_s
    end

    # Add speakers if available
    if result[:speakers] && !result[:speakers].empty?
      message_data['speakers'] = result[:speakers].join(',')
    end

    # Publish to Redis Stream
    stream_id = client.call('XADD', @stream_key, '*', *message_data.flatten)

    if result[:is_final]
      @logger.info "Published final transcription: #{result[:text][0..50]}..."
      @logger.info "  Stream ID: #{stream_id}"
    elsif ENV['DEBUG']
      @logger.debug "Published partial: #{result[:text][0..30]}..."
    end

    # Trim stream to keep last 1000 messages
    client.call('XTRIM', @stream_key, 'MAXLEN', '~', '1000')

  rescue => e
    @logger.error "Failed to publish to Redis: #{e.message}"
  end

  def monitor_services
    @logger.info "Monitoring services..."

    while @running
      # Check if components are still running
      unless @audio_stream&.running?
        @logger.error "Audio stream stopped unexpectedly"
        break
      end

      unless @transcribe_client&.running?
        @logger.error "Transcribe client stopped unexpectedly"
        break
      end

      # Print status every 10 seconds
      if Time.now.to_i % 10 == 0
        buffer_size = @audio_stream.buffer.size
        results_count = @transcribe_client.results.size
        @logger.info "[Status] Buffer: #{buffer_size} bytes, Results: #{results_count}"
      end

      sleep(1)
    end

    @logger.info "Monitor loop ended"
    stop
  end
end
