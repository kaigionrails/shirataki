require 'aws-sdk-transcribestreamingservice'
require 'concurrent'
require 'json'
require 'logger'

class TranscribeClient
  attr_reader :results

  def initialize(language_code: nil, region: nil, logger: nil)
    @language_code = language_code || ENV.fetch('TRANSCRIBE_LANGUAGE', 'ja-JP')
    @region = region || ENV.fetch('AWS_REGION', 'ap-northeast-1')
    @results = Concurrent::Array.new
    @running = Concurrent::AtomicBoolean.new(false)
    @client = nil
    @stream = nil
    @logger = logger || Logger.new(STDOUT).tap do |log|
      log.formatter = proc do |severity, datetime, progname, msg|
        "[#{datetime.strftime('%Y-%m-%d %H:%M:%S')}] [TranscribeClient] #{severity}: #{msg}\n"
      end
    end
  end

  def start(audio_stream)
    return if @running.true?

    @running.make_true
    @audio_stream = audio_stream

    begin
      initialize_client
      start_transcription_async
    rescue => e
      @logger.error "Failed to start: #{e.message}"
      @running.make_false
      raise
    end
  end

  def stop
    return unless @running.true?

    @logger.info "Stopping transcription..."
    @running.make_false

    if @input_stream
      begin
        @input_stream.signal_end_stream
      rescue HTTP2::Error::StreamClosed => e
        # This is expected during shutdown - the stream may already be closed
        @logger.debug "Stream already closed during shutdown" if ENV['DEBUG']
      rescue => e
        @logger.error "Error signaling end: #{e.message}"
      end
    end

    # Kill threads
    @audio_thread.kill if @audio_thread && @audio_thread.alive?
    @monitor_thread.kill if @monitor_thread && @monitor_thread.alive?
  end

  def running?
    @running.value
  end

  private

  def initialize_client
    @client = Aws::TranscribeStreamingService::AsyncClient.new(
      region: @region,
    )
  end

  def start_transcription_async
    @logger.info "Starting transcription with language: #{@language_code}"

    request_params = build_request_params

    # Create input stream for sending audio
    input_stream = Aws::TranscribeStreamingService::EventStreams::AudioStream.new
    @input_stream = input_stream

    # Create output stream and set up handlers
    output_stream = Aws::TranscribeStreamingService::EventStreams::TranscriptResultStream.new
    @output_stream = output_stream

    # Set up output stream event handlers (like serve.rb)
    output_stream.on_transcript_event_event do |event|
      process_transcript_event(event)
    end

    output_stream.on_bad_request_exception_event do |event|
      @logger.error "Bad request: #{event.message}"
      @running.make_false
    end

    output_stream.on_limit_exceeded_exception_event do |event|
      @logger.error "Limit exceeded: #{event.message}"
      @running.make_false
    end

    output_stream.on_internal_failure_exception_event do |event|
      @logger.error "Internal failure: #{event.message}"
      @running.make_false
    end

    output_stream.on_conflict_exception_event do |event|
      @logger.error "Conflict: #{event.message}"
      @running.make_false
    end

    output_stream.on_service_unavailable_exception_event do |event|
      @logger.error "Service unavailable: #{event.message}"
      @running.make_false
    end

    output_stream.on_error_event do |event|
      @logger.error "Stream error: #{event.inspect}"
      @running.make_false
    end

    # Add input_event_stream_handler to params
    request_params[:input_event_stream_handler] = input_stream
    request_params[:output_event_stream_handler] = output_stream


    # Start the transcription stream (like serve.rb)
    @logger.info "Initializing transcription stream..."
    @async_response = @client.start_stream_transcription(request_params)

    # Now that the stream is established, start sending audio
    @logger.info "Starting audio transmission..."
    @audio_thread = Thread.new { send_audio_chunks(input_stream) }

    # Start a monitoring thread instead of blocking
    @monitor_thread = Thread.new do
      begin
        @logger.info "Monitoring transcription stream..."
        result = @async_response.wait
        @logger.info "Transcription stream completed: #{result.inspect}"
      rescue => e
        @logger.error "Transcription stream error: #{e.message}"
        @running.make_false
      ensure
        # Ensure audio thread is stopped
        @running.make_false
        @audio_thread.kill if @audio_thread && @audio_thread.alive?
      end
    end

    @logger.info "Transcription started successfully (non-blocking)"
  end

  def build_request_params
    params = {
      language_code: @language_code,
      media_sample_rate_hertz: 16000,
      media_encoding: 'pcm',
      enable_partial_results_stabilization: true,
      partial_results_stability: 'high'
    }

    # Add vocabulary if specified
    if ENV['TRANSCRIBE_VOCABULARY']
      params[:vocabulary_name] = ENV['TRANSCRIBE_VOCABULARY']
    end

    # Enable speaker identification for supported languages
    if ['en-US', 'en-GB', 'es-US', 'fr-CA', 'fr-FR', 'de-DE'].include?(@language_code)
      params[:show_speaker_label] = true
      params[:enable_channel_identification] = false
    end

    params
  end

  def send_audio_chunks(stream)
    @logger.info "Starting to send audio chunks..."
    chunks_sent = 0
    total_bytes_sent = 0

    while @running.value
      begin
        # Read 200ms of audio (6400 bytes at 16kHz 16-bit mono)
        # AWS Transcribe recommends chunks between 200-500ms for optimal performance
        chunk = @audio_stream.read_chunk(32000)

        if chunk && !chunk.empty?
          chunk_size = chunk.bytesize

          # Send audio event
          stream.signal_audio_event_event(
            audio_chunk: chunk
          )
          chunks_sent += 1
          total_bytes_sent += chunk_size

          # Log progress every 5 chunks (1 second)
          if chunks_sent % 5 == 0
            @logger.info "Progress: Sent #{chunks_sent} chunks, #{total_bytes_sent} bytes total (#{chunks_sent * 1000}ms)"
          end

          # Small delay to prevent overwhelming the API
          sleep(0.18) # Slightly less than 200ms to account for processing time
        else
          # No audio available, wait a bit
          @logger.debug "No audio available, buffer size: #{@audio_stream.buffer.size}" if ENV['DEBUG']
          sleep(0.1)
        end

      rescue HTTP2::Error::StreamClosed => e
        @logger.error "AWS Transcribe stream unexpectedly closed: #{e.message}"
        @running.make_false
        break
      rescue => e
        @logger.error "Error sending audio: #{e.message}"
        @running.make_false
        break
      end
    end

    # Signal end of stream
    begin
      stream.signal_end_stream if stream && @running.value
      @logger.info "Signaled end of audio stream"
    rescue => e
      @logger.debug "Error signaling end: #{e.message}" if ENV['DEBUG']
    end
  end

  def process_transcript_event(event)
    return unless event.transcript

    event.transcript.results.each do |result|
      next if result.alternatives.empty?

      alternative = result.alternatives.first
      text = alternative.transcript
      @logger.debug "Received transcript: #{result.inspect}" if ENV['DEBUG']

      # Skip empty results
      next if text.nil? || text.strip.empty?

      # Build result object
      result_data = {
        text: text,
        is_final: !result.is_partial,
        timestamp: Time.now.iso8601,
        language: @language_code.split('-').first
      }

      # Add confidence score if available
      if alternative.items && !alternative.items.empty?
        confidences = alternative.items.map(&:confidence).compact
        if confidences.any?
          result_data[:confidence] = confidences.sum / confidences.size.to_f
        end
      end

      # Add speaker label if available
      if alternative.items && alternative.items.any? { |item| item.speaker }
        speakers = alternative.items.map(&:speaker).compact.uniq
        result_data[:speakers] = speakers if speakers.any?
      end

      # Add to results
      @results << result_data

      # Log the result
      if result_data[:is_final]
        @logger.info "Final: #{text}"
      else
        @logger.info "Partial: #{text}"
      end

      # Keep results array size manageable
      if @results.size > 1000
        @results.shift(100)
      end
    end
  rescue => e
    @logger.error "Error processing transcript: #{e.message}"
  end
end
