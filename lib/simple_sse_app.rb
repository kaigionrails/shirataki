require 'async'
require 'rack'

class SimpleSSEApp
  def call(env)
    request = Rack::Request.new(env)

    case request.path
    when '/sse'
      handle_sse_stream
    when '/health'
      handle_health_check
    else
      [404, {'Content-Type' => 'text/plain'}, ['Not Found']]
    end
  end

  private

  def handle_sse_stream
    headers = {
      'Content-Type' => 'text/event-stream',
      'Cache-Control' => 'no-cache',
      'Connection' => 'keep-alive',
      'Access-Control-Allow-Origin' => '*',
      'Access-Control-Allow-Headers' => 'Cache-Control',
      'X-Accel-Buffering' => 'no'
    }

    [200, headers, SSEStream.new]
  end

  def handle_health_check
    [200, {'Content-Type' => 'application/json'}, ["{\"status\":\"ok\",\"timestamp\":\"#{Time.now.iso8601}\"}"]]
  end
end

class SSEStream
  def each
    # 初期接続メッセージ
    yield "event: connected\n"
    yield "data: {\"message\":\"SSE connection established\",\"timestamp\":\"#{Time.now.iso8601}\"}\n\n"

    # 5秒ごとに固定メッセージを送信
    count = 0
    loop do
      count += 1

      # 固定メッセージを送信
      yield "event: message\n"
      yield "data: {\"count\":#{count},\"message\":\"Hello from SSE! Message ##{count}\",\"timestamp\":\"#{Time.now.iso8601}\"}\n\n"

      # 30秒ごとにハートビート
      if count % 6 == 0
        yield "event: heartbeat\n"
        yield "data: {\"timestamp\":\"#{Time.now.iso8601}\"}\n\n"
      end

      sleep 5
    end
  rescue => e
    yield "event: error\n"
    yield "data: {\"error\":\"#{e.message}\"}\n\n"
  end
end