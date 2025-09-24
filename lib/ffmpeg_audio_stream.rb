require 'open3'
require 'concurrent'
require 'logger'
require 'sentry-ruby'

class FFmpegAudioStream
  attr_reader :buffer

  def initialize(rtmp_url: nil, input_format: nil, logger: nil)
    @rtmp_url = rtmp_url || ENV.fetch('RTMP_URL', 'rtmp://localhost:1935/live')
    @input_format = input_format
    @buffer = Concurrent::Array.new
    @running = Concurrent::AtomicBoolean.new(false)
    @ffmpeg_process = nil
    @threads = []
    @logger = logger || Logger.new(STDOUT).tap do |log|
      log.formatter = proc do |severity, datetime, progname, msg|
        "[#{datetime.strftime('%Y-%m-%d %H:%M:%S')}] [FFmpegAudioStream] #{severity}: #{msg}\n"
      end
    end
  end

  def start
    return if @running.true?

    @running.make_true
    start_ffmpeg
  end

  def stop
    return unless @running.true?

    @logger.info "Stopping audio stream..."
    @running.make_false

    if @ffmpeg_process
      begin
        Process.kill('TERM', @ffmpeg_process.pid)
        @ffmpeg_process.value
      rescue => e
        @logger.error "Error stopping FFmpeg: #{e.message}"
        Sentry.capture_exception(e)
      end
    end

    @threads.each(&:kill)
    @threads.clear
    @buffer.clear
  end

  def running?
    @running.value
  end

  def read_chunk(size = 32000)
    return nil unless running?

    chunk = []
    size.times do
      break if @buffer.empty?
      byte = @buffer.shift
      chunk << byte if byte
    end

    return nil if chunk.empty?
    chunk.pack('C*')
  end

  private

  def start_ffmpeg
    # FFmpeg command to convert RTMP stream to PCM audio
    # Output format: PCM 16kHz, 16-bit, mono, little-endian
    cmd = build_ffmpeg_command

    @logger.info "Starting FFmpeg with command:"
    @logger.info "  #{cmd.join(' ')}"

    @stdin, @stdout, @stderr, @ffmpeg_process = Open3.popen3(*cmd)
    @stdout.binmode

    # Start threads to read FFmpeg output
    @threads << Thread.new { read_audio_stream }
    @threads << Thread.new { read_error_stream }

    @logger.info "FFmpeg started with PID: #{@ffmpeg_process.pid}"
  end

  def build_ffmpeg_command
    # Check if sudo is needed for binding to all interfaces
    cmd = if @rtmp_url.include?('0.0.0.0')
            @logger.info "Detected 0.0.0.0 in RTMP URL, using sudo for ffmpeg"
            ['sudo', 'ffmpeg']
          else
            ['ffmpeg']
          end

    # Input options
    if @input_format == 'test'
      # Generate test audio (sine wave)
      cmd += [
        '-f', 'lavfi',
        '-i', 'sine=frequency=440:duration=60'
      ]
    else
      # RTMP input
      cmd += [
        '-listen', '1',
        '-f',  'flv',
        '-i', @rtmp_url,
      ]
    end

    # Output options - PCM audio for Amazon Transcribe
    cmd += [
      '-f', 's16le',        # 16-bit little-endian PCM
      '-acodec', 'pcm_s16le',
      '-ar', '16000',       # 16kHz sample rate
      '-ac', '1',           # Mono
      '-vn',                # No video
      'pipe:1'              # Output to stdout
    ]

    cmd
  end

  def read_audio_stream
    while @running.value
      begin
        # Read audio data in chunks
        chunk = @stdout.read(32000)
        break unless chunk

        # Add bytes to buffer
        chunk.bytes.each { |byte| @buffer << byte }

        # Keep buffer size reasonable (max ~10 seconds of audio)
        if @buffer.size > 32000
          # Remove oldest data
          (@buffer.size - 32000).times { @buffer.shift }
        end

      rescue => e
        @logger.error "Error reading audio: #{e.message}"

        Sentry.capture_exception(e) do |scope|
          scope.set_tag('component', 'ffmpeg_audio_reader')
          scope.set_context('stream', {
            buffer_size: @buffer.size,
            rtmp_url: @rtmp_url
          })
        end

        break
      end
    end
  rescue => e
    @logger.error "Audio thread error: #{e.message}"

    Sentry.capture_exception(e) do |scope|
      scope.set_tag('component', 'ffmpeg_audio_thread')
    end
  ensure
    @logger.info "Audio stream thread stopped"
  end

  def read_error_stream
    while @running.value
      begin
        line = @stderr.gets
        break unless line

        # Only log important FFmpeg messages
        if line.include?('error') || line.include?('Error')
          @logger.error "[FFmpeg] #{line.strip}"

          # Report critical FFmpeg errors to Sentry
          if line.downcase.include?('fatal')
            Sentry.capture_message(
              "FFmpeg fatal error: #{line.strip}",
              level: 'error'
            )
          end
        elsif line.include?('warning') || line.include?('Warning')
          @logger.warn "[FFmpeg] #{line.strip}"
        elsif ENV['DEBUG']
          @logger.debug "[FFmpeg] #{line.strip}"
        end

      rescue => e
        @logger.error "Error reading stderr: #{e.message}"

        Sentry.capture_exception(e) do |scope|
          scope.set_tag('component', 'ffmpeg_stderr_reader')
        end

        break
      end
    end
  rescue => e
    @logger.error "Error thread error: #{e.message}"

    Sentry.capture_exception(e) do |scope|
      scope.set_tag('component', 'ffmpeg_error_thread')
    end
  ensure
    @logger.info "Error stream thread stopped"
  end
end
