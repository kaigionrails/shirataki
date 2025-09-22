require 'bundler/setup'

# Initialize Sentry before loading the app
require_relative 'config/sentry'

require_relative 'lib/redis_streams_sse_app'

# Wrap the app with Sentry's Rack middleware
use Sentry::Rack::CaptureExceptions

app = RedisStreamsSSEApp.new
run app
