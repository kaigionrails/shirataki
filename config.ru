require 'bundler/setup'
require_relative 'lib/simple_sse_app'

app = SimpleSSEApp.new
run app
