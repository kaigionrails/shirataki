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
    # Parse Redis URL (supports redis:// and rediss:// for TLS)
    redis_url = ENV.fetch('REDIS_URL', 'redis://localhost:6379/0')
    @redis_endpoint = Async::Redis::Endpoint.parse(redis_url)
    @stream_key = ENV.fetch('REDIS_STREAM_KEY', 'transcription_stream')
    @running = false
    @redis_task = nil
    @translation_queue = Concurrent::Array.new
    @translated_texts = Concurrent::Hash.new  # Track already translated text to avoid duplicates
    @min_translation_length = ENV.fetch('MIN_TRANSLATION_LENGTH', '20').to_i  # Minimum characters for translation
    @translation_batch_timeout = ENV.fetch('TRANSLATION_BATCH_TIMEOUT', '2').to_f  # Batch timeout in seconds
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

        # More detailed logging for debugging
        if wait_count % 2 == 0 || ENV['DEBUG']
          @logger.info "Waiting for audio data... (buffer size: #{@audio_stream.buffer.size}, wait: #{wait_count}s)"

          # Check if FFmpeg is still running
          unless @audio_stream.running?
            @logger.error "Audio stream stopped while waiting for data!"
            raise "FFmpeg audio stream stopped unexpectedly"
          end
        end
      end

      if @audio_stream.buffer.size < 6400
        @logger.warn "Starting with insufficient audio buffer (size: #{@audio_stream.buffer.size})"
        @logger.warn "This might indicate FFmpeg is not receiving RTMP data"
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
      consecutive_errors = 0
      max_consecutive_errors = 10
      published_count = 0
      last_stats_time = Time.now
      stats_interval = 60  # 1 minute

      @logger.info "Redis publisher started"
      last_loop_log = Time.now
      loop_count = 0

      while @running
        begin
          loop_count += 1

          # Log loop execution every 60 seconds to detect if loop is stuck
          if Time.now - last_loop_log > 60
            @logger.debug "[Redis Publisher Loop] Still running... (iteration: #{loop_count}, last_processed: #{last_processed_index})" if ENV['DEBUG']
            last_loop_log = Time.now
          end

          # Check for new transcription results
          results = @transcribe_client.results
          current_results_size = results.size

          @logger.debug "Checking results: current size=#{current_results_size}, last_processed=#{last_processed_index}" if ENV['DEBUG']

          # Log statistics periodically
          if Time.now - last_stats_time > stats_interval
            @logger.info "[Redis Publisher Stats] Published: #{published_count} messages, Queue size: #{@translation_queue.size}, Results buffer: #{current_results_size}"
            last_stats_time = Time.now
          end

          if current_results_size > last_processed_index
            # Process new results
            new_results = results[last_processed_index..-1]
            @logger.debug "Processing #{new_results.size} new transcription results" if ENV['DEBUG']

            # Use persistent Redis connection with reconnection logic
            redis_connected = false
            retry_count = 0
            max_retries = 3

            while !redis_connected && retry_count < max_retries
              begin
                # Add timeout for Redis operations
                Async do |task|
                  task.with_timeout(10) do  # 10 second timeout for Redis operations
                    Async::Redis::Client.open(@redis_endpoint) do |client|
                      # Test connection with PING
                      ping_result = client.call('PING')
                      if ping_result != 'PONG'
                        raise "Redis ping failed: #{ping_result}"
                      end

                      @logger.debug "Redis client connected successfully" if ENV['DEBUG']
                      redis_connected = true

                      new_results.each do |result|
                        # Publish original transcription
                        publish_to_redis(client, result)
                        published_count += 1

                        # Queue for translation if enabled and text is long enough
                        if @translate_client&.enabled? && should_translate?(result)
                          @translation_queue << result
                        end
                      end
                    end
                  end
                end.wait

                # Reset consecutive errors on success
                consecutive_errors = 0

              rescue Async::TimeoutError => e
                retry_count += 1
                @logger.error "Redis operation timeout (attempt #{retry_count}/#{max_retries})"

                if retry_count < max_retries
                  sleep_time = retry_count * 2  # Exponential backoff
                  @logger.info "Retrying Redis connection in #{sleep_time} seconds..."
                  sleep(sleep_time)
                else
                  raise "Redis operation timeout after #{max_retries} attempts"
                end
              rescue => e
                retry_count += 1
                @logger.error "Redis connection attempt #{retry_count}/#{max_retries} failed: #{e.message}"

                if retry_count < max_retries
                  sleep_time = retry_count * 2  # Exponential backoff
                  @logger.info "Retrying Redis connection in #{sleep_time} seconds..."
                  sleep(sleep_time)
                else
                  raise e  # Re-raise if all retries exhausted
                end
              end
            end

            last_processed_index = current_results_size
            @logger.debug "Updated last_processed_index to #{last_processed_index}" if ENV['DEBUG']
          end

          # Check every 100ms
          sleep(0.1)

        rescue => e
          consecutive_errors += 1
          @logger.error "Redis publisher error (#{consecutive_errors}/#{max_consecutive_errors}): #{e.message}"
          @logger.error e.backtrace.first(3).join("\n") if e.backtrace

          # Report to Sentry
          Sentry.capture_exception(e) do |scope|
            scope.set_tag('component', 'redis_publisher')
            scope.set_context('redis', {
              endpoint: @redis_endpoint.to_s,
              stream_key: @stream_key,
              consecutive_errors: consecutive_errors,
              published_count: published_count
            })
          end

          # Check if too many consecutive errors
          if consecutive_errors >= max_consecutive_errors
            @logger.fatal "Too many consecutive Redis errors (#{consecutive_errors}), stopping service"
            @running = false
            break
          end

          # Exponential backoff for errors
          sleep_time = [consecutive_errors * 2, 30].min  # Max 30 seconds
          @logger.info "Waiting #{sleep_time} seconds before retry..."
          sleep(sleep_time)
        end
      end

      @logger.info "Redis publisher stopped (published #{published_count} total messages)"
    end
  end

  def start_translation_processor
    @translation_task = Async do
      @logger.info "Translation processor started"
      @logger.info "Minimum translation length: #{@min_translation_length} characters"
      @logger.info "Batch size: #{@translate_client.batch_size}, Timeout: #{@translation_batch_timeout}s"
      @logger.info "Translate only final: #{@translate_client.translate_only_final?}"

      batch = []
      last_batch_time = Time.now

      while @running
        begin
          # Collect items for batch processing
          while @translation_queue.size > 0 && batch.size < @translate_client.batch_size
            result = @translation_queue.shift

            # Skip non-final texts if configured
            if @translate_client.translate_only_final? && !result[:is_final]
              @logger.debug "Skipping non-final text for translation" if ENV['DEBUG']
              next
            end

            # Skip if we've already translated this exact text
            text_hash = Digest::MD5.hexdigest(result[:text])
            if @translated_texts[text_hash]
              @logger.debug "Skipping already translated text: #{result[:text][0..30]}..." if ENV['DEBUG']
              next
            end

            batch << result
          end

          # Process batch if it's full or timeout reached
          should_process = batch.size >= @translate_client.batch_size ||
                          (batch.size > 0 && Time.now - last_batch_time >= @translation_batch_timeout)

          if should_process && batch.size > 0
            @logger.info "Processing translation batch: #{batch.size} texts"

            # Extract texts for batch translation
            texts_to_translate = batch.map { |r| r[:text] }

            # Batch translate
            translated_texts = @translate_client.translate_batch(texts_to_translate)

            # Process results
            batch.each_with_index do |result, index|
              translated_text = translated_texts[index]

              if translated_text
                # Mark this text as translated
                text_hash = Digest::MD5.hexdigest(result[:text])
                @translated_texts[text_hash] = true

                # Create a new result for the translation
                translation_result = {
                  text: translated_text,
                  is_final: result[:is_final],
                  timestamp: Time.now.iso8601,
                  language: 'en',
                  original_language: result[:language] || 'ja',
                  translation_of: result[:text],
                  type: 'translation'
                }

                # Publish the translation to Redis
                Async::Redis::Client.open(@redis_endpoint) do |client|
                  publish_to_redis(client, translation_result)
                end
              end
            end

            # Clean up old entries if cache gets too large
            if @translated_texts.size > 1000
              @translated_texts.clear
              @logger.debug "Cleared translation cache" if ENV['DEBUG']
            end

            # Reset batch
            batch.clear
            last_batch_time = Time.now
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

    # Validate result before publishing
    unless result[:text] && !result[:text].empty?
      @logger.warn "Skipping empty result publication"
      return
    end

    # Prepare message for Redis Stream
    message_data = {
      'room' => @room,
      'language' => result[:language] || 'ja',
      'text' => result[:text],
      'is_final' => result[:is_final].to_s,
      'timestamp' => result[:timestamp] || Time.now.iso8601,
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

    # Retry logic for XADD command
    retry_count = 0
    max_retries = 3

    while retry_count < max_retries
      begin
        # Publish to Redis Stream with MAXLEN to automatically cap the stream size
        # Using '~' for approximate trimming is more efficient than exact trimming
        stream_id = client.call('XADD', @stream_key, 'MAXLEN', '~', '1000', '*', *message_data.flatten)

        if stream_id.nil? || stream_id.empty?
          raise "XADD returned invalid stream ID: #{stream_id.inspect}"
        end

        # Log final results and translations
        if result[:is_final]
          if result[:type] == 'translation'
            @logger.info "Published translation: #{result[:text][0..50]}..."
          else
            @logger.info "Published final transcription: #{result[:text][0..50]}..."
          end
        elsif ENV['DEBUG']
          @logger.debug "Published partial (ID: #{stream_id}): #{result[:text][0..30]}..."
        end

        # Success - exit retry loop
        break

      rescue => e
        retry_count += 1
        @logger.error "Failed to publish to Redis (attempt #{retry_count}/#{max_retries}): #{e.message}"

        if retry_count < max_retries
          sleep(retry_count * 0.5)  # Small backoff
        else
          # Report to Sentry only after all retries exhausted
          Sentry.capture_exception(e) do |scope|
            scope.set_tag('component', 'redis_publish')
            scope.set_context('message', result)
            scope.set_context('retry_info', {
              attempts: retry_count,
              stream_key: @stream_key
            })
          end

          # Re-raise to trigger connection retry in parent
          raise e
        end
      end
    end
  end

  def monitor_services
    @logger.info "Monitoring services..."
    last_status_time = Time.now
    status_interval = 10

    while @running
      # Check if components are still running
      unless @audio_stream&.running?
        @logger.error "Audio stream stopped unexpectedly"
        @logger.error "Final buffer size: #{@audio_stream.buffer.size}" if @audio_stream

        # Report to Sentry
        Sentry.capture_message(
          "Audio stream stopped unexpectedly",
          level: 'error'
        )

        break
      end

      unless @transcribe_client&.running?
        @logger.error "Transcribe client stopped unexpectedly"
        @logger.error "Buffer size when stopped: #{@audio_stream.buffer.size}" if @audio_stream

        # Report to Sentry
        Sentry.capture_message(
          "Transcribe client stopped unexpectedly",
          level: 'error'
        )

        break
      end

      # Print status at regular intervals
      if Time.now - last_status_time >= status_interval
        buffer_size = @audio_stream.buffer.size
        results_count = @transcribe_client.results.size
        @logger.info "[Status] Buffer: #{buffer_size} bytes, Results: #{results_count}, FFmpeg: #{@audio_stream.running? ? 'running' : 'stopped'}"

        # Warn if buffer is consistently empty
        if buffer_size == 0
          @logger.warn "Audio buffer is empty - FFmpeg may not be receiving RTMP data"
        end

        last_status_time = Time.now
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
