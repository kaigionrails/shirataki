#!/usr/bin/env ruby

require 'aws-sdk-transcribestreamingservice'
require 'logger'

# Setup logger
logger = Logger.new(STDOUT)
logger.formatter = proc do |severity, datetime, progname, msg|
  "[#{datetime.strftime('%Y-%m-%d %H:%M:%S')}] #{severity}: #{msg}\n"
end

begin
  # Configuration
  region = ENV.fetch('AWS_REGION', 'ap-northeast-1')
  language_code = ENV.fetch('TRANSCRIBE_LANGUAGE', 'ja-JP')

  logger.info "Testing AWS Transcribe Streaming Service"
  logger.info "Region: #{region}"
  logger.info "Language: #{language_code}"

  # Check credentials
  if ENV['AWS_ACCESS_KEY_ID']
    logger.info "Using AWS credentials from environment variables"
  elsif ENV['AWS_PROFILE']
    logger.info "Using AWS profile: #{ENV['AWS_PROFILE']}"
  else
    logger.info "Using default credential chain (IAM role, etc.)"
  end

  # Initialize client
  logger.info "Initializing Transcribe client..."
  client = Aws::TranscribeStreamingService::AsyncClient.new(
    region: region
  )
  logger.info "Client initialized successfully"

  # Prepare request parameters
  request_params = {
    language_code: language_code,
    media_sample_rate_hertz: 16000,
    media_encoding: 'pcm'
  }

  # Add optional parameters if they exist
  if ENV['TRANSCRIBE_VOCABULARY']
    request_params[:vocabulary_name] = ENV['TRANSCRIBE_VOCABULARY']
    logger.info "Using vocabulary: #{ENV['TRANSCRIBE_VOCABULARY']}"
  end

  if ENV['TRANSCRIBE_LANGUAGE_MODEL_NAME']
    request_params[:language_model_name] = ENV['TRANSCRIBE_LANGUAGE_MODEL_NAME']
    logger.info "Using language model: #{ENV['TRANSCRIBE_LANGUAGE_MODEL_NAME']}"
  end

  # Create event streams
  logger.info "Creating event streams..."
  input_stream = Aws::TranscribeStreamingService::EventStreams::AudioStream.new
  output_stream = Aws::TranscribeStreamingService::EventStreams::TranscriptResultStream.new

  # Setup output handlers
  output_stream.on_transcript_event_event do |event|
    logger.info "Received transcript event"
    if event.transcript && event.transcript.results
      event.transcript.results.each do |result|
        if result.alternatives && !result.alternatives.empty?
          text = result.alternatives.first.transcript
          logger.info "Transcript: #{text}"
        end
      end
    end
  end

  output_stream.on_bad_request_exception_event do |event|
    logger.error "Bad request: #{event.message}"
  end

  output_stream.on_limit_exceeded_exception_event do |event|
    logger.error "Limit exceeded: #{event.message}"
  end

  output_stream.on_internal_failure_exception_event do |event|
    logger.error "Internal failure: #{event.message}"
  end

  output_stream.on_conflict_exception_event do |event|
    logger.error "Conflict: #{event.message}"
  end

  output_stream.on_service_unavailable_exception_event do |event|
    logger.error "Service unavailable: #{event.message}"
  end

  output_stream.on_error_event do |event|
    logger.error "Stream error: #{event.inspect}"
  end

  # Add stream handlers to request
  request_params[:input_event_stream_handler] = input_stream
  request_params[:output_event_stream_handler] = output_stream

  # Start transcription stream
  logger.info "Starting transcription stream..."
  logger.info "Request parameters: #{request_params.reject { |k, _| k.to_s.include?('stream') }.inspect}"

  async_response = client.start_stream_transcription(request_params)

  logger.info "Stream started successfully!"

  # Send test audio (silence)
  logger.info "Sending test audio (silence)..."

  # Create 1 second of silence (16000 Hz, 16-bit, mono = 32000 bytes)
  silence = "\x00" * 32000

  # Send a few chunks
  3.times do |i|
    logger.info "Sending chunk #{i + 1}..."
    input_stream.signal_audio_event_event(audio_chunk: silence)
    sleep(1)
  end

  # Signal end of stream
  logger.info "Signaling end of stream..."
  input_stream.signal_end_stream

  # Wait for response
  logger.info "Waiting for response..."
  result = async_response.wait

  logger.info "Test completed successfully!"
  logger.info "Response: #{result.inspect}"

rescue Aws::Errors::MissingCredentialsError => e
  logger.error "AWS credentials not found!"
  logger.error "Error: #{e.message}"
  logger.error "Please ensure AWS credentials are configured:"
  logger.error "  - Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables"
  logger.error "  - Or use AWS_PROFILE"
  logger.error "  - Or use IAM role if running on EC2"
  exit 1

rescue Aws::TranscribeStreamingService::Errors::ServiceError => e
  logger.error "AWS Transcribe service error!"
  logger.error "Error class: #{e.class.name}"
  logger.error "Error message: #{e.message}"
  logger.error "Error code: #{e.code}" if e.respond_to?(:code)
  logger.error "Status code: #{e.status_code}" if e.respond_to?(:status_code)
  exit 1

rescue => e
  logger.error "Unexpected error!"
  logger.error "Error class: #{e.class.name}"
  logger.error "Error message: #{e.message}"
  logger.error "Backtrace:"
  e.backtrace.first(5).each { |line| logger.error "  #{line}" }
  exit 1
end