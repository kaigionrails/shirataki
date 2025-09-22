require 'sentry-ruby'

Sentry.init do |config|
  # Set DSN from environment variable
  config.dsn = ENV['SENTRY_DSN']

  # Set the environment
  config.environment = ENV.fetch('SENTRY_ENVIRONMENT', 'development')

  # Set the release if available
  config.release = ENV['SENTRY_RELEASE'] if ENV['SENTRY_RELEASE']

  # Breadcrumbs configuration
  config.breadcrumbs_logger = [:sentry_logger, :http_logger]

  # Sample rate for performance monitoring (0.0 to 1.0)
  config.traces_sample_rate = ENV.fetch('SENTRY_TRACES_SAMPLE_RATE', '0.1').to_f

  # Sample rate for profiling (0.0 to 1.0)
  config.profiles_sample_rate = ENV.fetch('SENTRY_PROFILES_SAMPLE_RATE', '0.1').to_f

  # Filter sensitive data
  config.before_send = lambda do |event, hint|
    # Filter out AWS credentials from the event
    if event.extra
      event.extra.deep_stringify_keys!
      event.extra.delete_if { |k, _| k.match?(/aws|key|secret|password|token/i) }
    end

    # Filter environment variables
    if event.request && event.request.env
      event.request.env.delete_if { |k, _| k.match?(/AWS|KEY|SECRET|PASSWORD|TOKEN/i) }
    end

    event
  end

  # Capture additional context
  config.before_breadcrumb = lambda do |breadcrumb, hint|
    # Add additional context to breadcrumbs if needed
    breadcrumb
  end
end

# Set global tags after initialization
Sentry.configure_scope do |scope|
  scope.set_tags(
    service: 'shirataki',
    component: ENV['SENTRY_COMPONENT'] || 'unknown'
  )
end

# Helper method to set user/room context
def set_sentry_context(room: nil, language: nil, **extra)
  Sentry.configure_scope do |scope|
    scope.set_context('transcription', {
      room: room,
      language: language,
      **extra
    })
  end
end

# Helper method to capture exceptions with additional context
def capture_with_context(exception, **context)
  Sentry.capture_exception(exception) do |scope|
    context.each do |key, value|
      scope.set_tag(key, value)
    end
  end
end
