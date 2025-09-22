require 'async'
require 'async/redis'
require 'json'
require 'logger'
require 'concurrent'
require 'digest'
require 'sentry-ruby'
require_relative 'ffmpeg_audio_stream'
require_relative 'transcribe_client'
require_relative 'bedrock_translate_client'

class RtmpTranscribeService
  def initialize(rtmp_url: nil, room: 'default', test_mode: false, logger: nil)
    @rtmp_url = rtmp_url || ENV.fetch('RTMP_URL', 'rtmp://localhost:1935/live')
    @room = room
    @test_mode = test_mode
    @audio_stream = nil
    @transcribe_client = nil
    @translate_client = nil
    @redis_endpoint = Async::Redis.local_endpoint(
      host: ENV.fetch('REDIS_HOST', 'localhost'),
      port: ENV.fetch('REDIS_PORT', 6379).to_i,
      db: ENV.fetch('REDIS_DB', 0).to_i
    )
    @stream_key = ENV.fetch('REDIS_STREAM_KEY', 'transcription_stream')
    @running = false
    @redis_task = nil
    @translation_queue = Concurrent::Array.new
    @translated_texts = Concurrent::Hash.new  # Track already translated text to avoid duplicates
    @min_translation_length = ENV.fetch('MIN_TRANSLATION_LENGTH', '20').to_i  # Minimum characters for translation
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

      # Start Transcribe client (non-blocking)
      @transcribe_client = TranscribeClient.new(logger: @logger)
      @transcribe_client.start(@audio_stream)

      # Start Translation client
      @translate_client = BedrockTranslateClient.new(logger: @logger)
      if @translate_client.enabled?
        @logger.info "Translation enabled"
        start_translation_processor
      else
        @logger.info "Translation disabled"
      end

      # Start Redis publishing task
      start_redis_publisher

      # Monitor loop
      monitor_services

    rescue => e
      @logger.error "Error: #{e.message}"
      @logger.error e.backtrace.first(5).join("\n") if e.backtrace

      # Report to Sentry with context
      Sentry.capture_exception(e) do |scope|
        scope.set_context('rtmp_service', {
          rtmp_url: @rtmp_url,
          room: @room,
          test_mode: @test_mode
        })
      end

      stop
    end
  end

  def stop
    return unless @running

    @logger.info "Stopping service..."
    @running = false

    # Stop components
    @redis_task&.stop
    @translation_task&.stop if @translation_task
    @transcribe_client&.stop
    @audio_stream&.stop

    @logger.info "Service stopped"
  end

  private

  def start_redis_publisher
    @redis_task = Async do
      last_processed_index = 0
      @logger.info "Redis publisher started"

      while @running
        begin
          # Check for new transcription results
          results = @transcribe_client.results
          @logger.debug "Checking results: current size=#{results.size}, last_processed=#{last_processed_index}" if ENV['DEBUG']

          if results.size > last_processed_index
            # Process new results
            new_results = results[last_processed_index..-1]
            @logger.info "Processing #{new_results.size} new transcription results"

            Async::Redis::Client.open(@redis_endpoint) do |client|
              @logger.debug "Redis client connected" if ENV['DEBUG']
              new_results.each do |result|
                # Publish original transcription
                publish_to_redis(client, result)

                # Queue for translation if enabled and text is long enough
                if @translate_client&.enabled? && should_translate?(result)
                  @translation_queue << result
                end
              end
            end

            last_processed_index = results.size
          end

          # Check every 100ms
          sleep(0.1)

        rescue => e
          @logger.error "Redis publisher error: #{e.message}"
          @logger.error e.backtrace.first(3).join("\n") if e.backtrace

          # Report to Sentry
          Sentry.capture_exception(e) do |scope|
            scope.set_tag('component', 'redis_publisher')
            scope.set_context('redis', {
              endpoint: @redis_endpoint.to_s,
              stream_key: @stream_key
            })
          end

          sleep(1)
        end
      end
      @logger.info "Redis publisher stopped"
    end
  end

  def start_translation_processor
    @translation_task = Async do
      @logger.info "Translation processor started"
      @logger.info "Minimum translation length: #{@min_translation_length} characters"

      while @running
        begin
          # Process translation queue
          if @translation_queue.size > 0
            result = @translation_queue.shift
            text_to_translate = result[:text]

            # Skip if we've already translated this exact text
            text_hash = Digest::MD5.hexdigest(text_to_translate)
            if @translated_texts[text_hash]
              @logger.debug "Skipping already translated text: #{text_to_translate[0..30]}..." if ENV['DEBUG']
              next
            end

            @logger.info "Translating #{result[:is_final] ? 'final' : 'partial'}: #{text_to_translate[0..50]}..."

            # Translate the text
            translated_text = @translate_client.translate(text_to_translate)

            if translated_text
              # Mark this text as translated
              @translated_texts[text_hash] = true

              # Clean up old entries if cache gets too large
              if @translated_texts.size > 1000
                @translated_texts.clear
                @logger.debug "Cleared translation cache" if ENV['DEBUG']
              end

              # Create a new result for the translation
              translation_result = {
                text: translated_text,
                is_final: result[:is_final],  # Preserve the original final state
                timestamp: Time.now.iso8601,
                language: 'en',  # English translation
                original_language: result[:language] || 'ja',
                translation_of: text_to_translate,
                type: 'translation'
              }

              # Publish the translation to Redis
              Async::Redis::Client.open(@redis_endpoint) do |client|
                publish_to_redis(client, translation_result)
              end
            end
          end

          # Check every 100ms
          sleep(0.1)

        rescue => e
          @logger.error "Translation processor error: #{e.message}"

          Sentry.capture_exception(e) do |scope|
            scope.set_tag('component', 'translation_processor')
          end

          sleep(1)
        end
      end

      @logger.info "Translation processor stopped"
    end
  end

  def publish_to_redis(client, result)
    @logger.debug "Publishing result to Redis: #{result[:text][0..50]}..." if ENV['DEBUG']

    # Prepare message for Redis Stream
    message_data = {
      'room' => @room,
      'language' => result[:language] || 'ja',
      'text' => result[:text],
      'is_final' => result[:is_final].to_s,
      'timestamp' => result[:timestamp],
      'type' => result[:type] || 'transcription'
    }

    # Add confidence if available
    if result[:confidence]
      message_data['confidence'] = result[:confidence].to_s
    end

    # Add speakers if available
    if result[:speakers] && !result[:speakers].empty?
      message_data['speakers'] = result[:speakers].join(',')
    end

    # Add translation-specific fields if this is a translation
    if result[:type] == 'translation'
      message_data['original_language'] = result[:original_language] if result[:original_language]
      message_data['translation_of'] = result[:translation_of] if result[:translation_of]
    end

    # Publish to Redis Stream
    stream_id = client.call('XADD', @stream_key, '*', *message_data.flatten)

    if result[:is_final]
      if result[:type] == 'translation'
        @logger.info "Published translation: #{result[:text][0..50]}..."
      else
        @logger.info "Published final transcription: #{result[:text][0..50]}..."
      end
      @logger.info "  Stream ID: #{stream_id}"
    elsif ENV['DEBUG']
      @logger.debug "Published partial: #{result[:text][0..30]}..."
    end

    # Trim stream to keep last 1000 messages
    client.call('XTRIM', @stream_key, 'MAXLEN', '~', '1000')

  rescue => e
    @logger.error "Failed to publish to Redis: #{e.message}"

    # Report to Sentry with message context
    Sentry.capture_exception(e) do |scope|
      scope.set_tag('component', 'redis_publish')
      scope.set_context('message', result)
    end
  end

  def monitor_services
    @logger.info "Monitoring services..."

    while @running
      # Check if components are still running
      unless @audio_stream&.running?
        @logger.error "Audio stream stopped unexpectedly"

        # Report to Sentry
        Sentry.capture_message(
          "Audio stream stopped unexpectedly",
          level: 'error'
        )

        break
      end

      unless @transcribe_client&.running?
        @logger.error "Transcribe client stopped unexpectedly"

        # Report to Sentry
        Sentry.capture_message(
          "Transcribe client stopped unexpectedly",
          level: 'error'
        )

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

  def should_translate?(result)
    text = result[:text]
    return false if text.nil? || text.strip.empty?

    # Translate if it's final OR if it's long enough
    if result[:is_final]
      @logger.debug "Queueing final text for translation" if ENV['DEBUG']
      return true
    elsif text.length >= @min_translation_length
      @logger.debug "Queueing partial text for translation (length: #{text.length})" if ENV['DEBUG']
      return true
    else
      @logger.debug "Skipping translation - text too short (length: #{text.length}, min: #{@min_translation_length})" if ENV['DEBUG']
      return false
    end
  end
end
