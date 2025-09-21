require 'bundler/setup'
require_relative 'lib/redis_streams_sse_app'

app = RedisStreamsSSEApp.new
run app
