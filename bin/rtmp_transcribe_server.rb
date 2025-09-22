#!/usr/bin/env ruby

require 'bundler/setup'
require 'optparse'

# Initialize Sentry
require_relative '../config/sentry'
ENV['SENTRY_COMPONENT'] = 'rtmp_transcribe_server'

require_relative '../lib/rtmp_transcribe_service'

# Parse command line options
options = {
  rtmp_url: ENV.fetch('RTMP_URL', 'rtmp://localhost:1935/live'),
  room: ENV.fetch('ROOM', 'default'),
  test_mode: false
}

OptionParser.new do |opts|
  opts.banner = "Usage: rtmp_transcribe_server.rb [options]"

  opts.on("-r", "--rtmp URL", "RTMP stream URL") do |url|
    options[:rtmp_url] = url
  end

  opts.on("-m", "--room ROOM", "Room name for transcriptions") do |room|
    options[:room] = room
  end

  opts.on("-t", "--test", "Use test audio instead of RTMP") do
    options[:test_mode] = true
  end

  opts.on("-h", "--help", "Show this help message") do
    puts opts
    exit
  end
end.parse!

# Override for test mode
if options[:test_mode]
  require_relative '../lib/ffmpeg_audio_stream'
  # Monkey patch for test mode
  class FFmpegAudioStream
    def build_ffmpeg_command
      cmd = ['ffmpeg']
      # Generate test audio (sine wave)
      cmd += [
        '-f', 'lavfi',
        '-i', 'sine=frequency=440:duration=600:sample_rate=16000'
      ]
      # Output options
      cmd += [
        '-f', 's16le',
        '-acodec', 'pcm_s16le',
        '-ar', '16000',
        '-ac', '1',
        '-vn',
        'pipe:1'
      ]
      cmd
    end
  end
  puts "TEST MODE: Using generated audio instead of RTMP stream"
end

# Check AWS credentials (skip in test mode)
if !options[:test_mode] && (ENV['AWS_ACCESS_KEY_ID'].nil? || ENV['AWS_SECRET_ACCESS_KEY'].nil?)
  puts "ERROR: AWS credentials not set"
  puts "Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables"
  puts "Note: You can use --test flag to run in test mode without AWS"
  exit 1
end

# Print configuration
puts "=" * 60
puts "RTMP to Amazon Transcribe Service"
puts "=" * 60
puts "Configuration:"
puts "  RTMP URL: #{options[:rtmp_url]}"
puts "  Room: #{options[:room]}"
puts "  Redis: #{ENV.fetch('REDIS_HOST', 'localhost')}:#{ENV.fetch('REDIS_PORT', 6379)}"
puts "  Stream Key: #{ENV.fetch('REDIS_STREAM_KEY', 'transcription_stream')}"
puts "  Language: #{ENV.fetch('TRANSCRIBE_LANGUAGE', 'ja-JP')}"
puts "  AWS Region: #{ENV.fetch('AWS_REGION', 'ap-northeast-1')}"
puts "=" * 60
puts ""

# Create and start service
service = RtmpTranscribeService.new(
  rtmp_url: options[:rtmp_url],
  room: options[:room],
  test_mode: options[:test_mode]
)

# Set up signal handlers
@shutdown = false

Signal.trap("INT") do
  puts "\nReceived INT signal, stopping..."
  @shutdown = true
end

Signal.trap("TERM") do
  puts "\nReceived TERM signal, stopping..."
  @shutdown = true
end

# Monitor for shutdown in separate thread
Thread.new do
  loop do
    if @shutdown
      service.stop
      exit 0
    end
    sleep 0.1
  end
end

# Start the service
begin
  # Set Sentry context for this service
  Sentry.configure_scope do |scope|
    scope.set_context('service', {
      rtmp_url: options[:rtmp_url],
      room: options[:room],
      test_mode: options[:test_mode]
    })
  end

  service.start
rescue => e
  puts "Fatal error: #{e.message}"
  puts e.backtrace.first(10)

  # Report to Sentry
  Sentry.capture_exception(e)

  exit 1
end
