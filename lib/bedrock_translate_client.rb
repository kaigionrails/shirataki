require 'aws-sdk-bedrockruntime'
require 'json'
require 'logger'
require 'sentry-ruby'
require 'concurrent'

class BedrockTranslateClient
  def initialize(region: nil, model_id: nil, logger: nil)
    @region = region || ENV.fetch('BEDROCK_REGION', 'ap-northeast-1')
    @model_id = model_id || ENV.fetch('BEDROCK_MODEL_ID', 'anthropic.claude-3-5-sonnet-20240620-v1:0')
    @enabled = ENV.fetch('ENABLE_TRANSLATION', 'false').downcase == 'true'

    # Cache for translations to avoid re-translating identical text
    @translation_cache = Concurrent::Hash.new
    @cache_max_size = 100

    # Batch processing configuration
    @batch_size = ENV.fetch('TRANSLATION_BATCH_SIZE', '5').to_i
    @translate_only_final = ENV.fetch('TRANSLATE_ONLY_FINAL', 'false').downcase == 'true'

    @logger = logger || Logger.new(STDOUT).tap do |log|
      log.formatter = proc do |severity, datetime, progname, msg|
        "[#{datetime.strftime('%Y-%m-%d %H:%M:%S')}] [BedrockTranslateClient] #{severity}: #{msg}\n"
      end
    end

    if @enabled
      initialize_client
      @logger.info "Translation enabled with model: #{@model_id}"
    else
      @logger.info "Translation disabled (set ENABLE_TRANSLATION=true to enable)"
    end
  end

  def enabled?
    @enabled
  end

  def translate_only_final?
    @translate_only_final
  end

  def batch_size
    @batch_size
  end

  def translate(text, source_lang: 'ja', target_lang: 'en')
    return nil unless @enabled
    return nil if text.nil? || text.strip.empty?

    # Check cache first
    cache_key = "#{source_lang}:#{target_lang}:#{text}"
    cached = @translation_cache[cache_key]
    if cached
      @logger.debug "Translation found in cache" if ENV['DEBUG']
      return cached
    end

    begin
      # Prepare the prompt for translation
      prompt = build_translation_prompt(text, source_lang, target_lang)

      # Build request body for Claude
      request_body = {
        anthropic_version: 'bedrock-2023-05-31',
        max_tokens: 2048,
        temperature: 0.3,  # Lower temperature for more consistent translations
        messages: [
          {
            role: 'user',
            content: prompt
          }
        ]
      }

      @logger.debug "Translating: #{text[0..50]}..." if ENV['DEBUG']

      # Invoke the model
      response = @client.invoke_model(
        model_id: @model_id,
        content_type: 'application/json',
        accept: 'application/json',
        body: request_body.to_json
      )

      # Parse the response
      response_body = JSON.parse(response.body.read)
      translated_text = extract_translation(response_body)

      # Cache the translation
      add_to_cache(cache_key, translated_text)

      @logger.info "Translated: #{text[0..30]}... -> #{translated_text[0..30]}..."

      translated_text

    rescue Aws::BedrockRuntime::Errors::ServiceError => e
      @logger.error "AWS Bedrock error: #{e.message}"

      Sentry.capture_exception(e) do |scope|
        scope.set_tag('component', 'bedrock_translate')
        scope.set_context('translation', {
          text: text[0..100],
          source_lang: source_lang,
          target_lang: target_lang,
          model_id: @model_id
        })
      end

      nil

    rescue => e
      @logger.error "Translation error: #{e.message}"

      Sentry.capture_exception(e) do |scope|
        scope.set_tag('component', 'bedrock_translate')
      end

      nil
    end
  end

  # Batch translation method to reduce API calls
  def translate_batch(texts_array, source_lang: 'ja', target_lang: 'en')
    return [] unless @enabled
    return [] if texts_array.nil? || texts_array.empty?

    # Filter out empty texts and check cache
    texts_to_translate = []
    cached_results = {}

    texts_array.each_with_index do |text, index|
      next if text.nil? || text.strip.empty?

      cache_key = "#{source_lang}:#{target_lang}:#{text}"
      cached = @translation_cache[cache_key]

      if cached
        cached_results[index] = cached
        @logger.debug "Translation #{index} found in cache" if ENV['DEBUG']
      else
        texts_to_translate << { index: index, text: text }
      end
    end

    # If all translations are cached, return them
    if texts_to_translate.empty?
      return texts_array.map.with_index { |_, i| cached_results[i] }
    end

    begin
      # Build batch translation prompt
      prompt = build_batch_translation_prompt(texts_to_translate, source_lang, target_lang)

      # Build request body for Claude
      request_body = {
        anthropic_version: 'bedrock-2023-05-31',
        max_tokens: 4096,  # Increased for batch processing
        temperature: 0.3,
        messages: [
          {
            role: 'user',
            content: prompt
          }
        ]
      }

      @logger.info "Batch translating #{texts_to_translate.size} texts"

      # Single API call for all texts
      response = @client.invoke_model(
        model_id: @model_id,
        content_type: 'application/json',
        accept: 'application/json',
        body: request_body.to_json
      )

      # Parse the response
      response_body = JSON.parse(response.body.read)
      translations = extract_batch_translations(response_body, texts_to_translate.size)

      # Cache the translations and build result array
      results = Array.new(texts_array.size)

      texts_to_translate.each_with_index do |item, batch_index|
        translation = translations[batch_index]
        if translation
          cache_key = "#{source_lang}:#{target_lang}:#{item[:text]}"
          add_to_cache(cache_key, translation)
          results[item[:index]] = translation
        end
      end

      # Add cached results
      cached_results.each { |index, text| results[index] = text }

      @logger.info "Batch translation completed: #{texts_to_translate.size} texts translated in 1 API call"

      results

    rescue Aws::BedrockRuntime::Errors::ServiceError => e
      @logger.error "AWS Bedrock batch error: #{e.message}"

      Sentry.capture_exception(e) do |scope|
        scope.set_tag('component', 'bedrock_translate_batch')
        scope.set_context('batch_translation', {
          batch_size: texts_to_translate.size,
          source_lang: source_lang,
          target_lang: target_lang,
          model_id: @model_id
        })
      end

      # Return empty translations on error
      Array.new(texts_array.size)

    rescue => e
      @logger.error "Batch translation error: #{e.message}"

      Sentry.capture_exception(e) do |scope|
        scope.set_tag('component', 'bedrock_translate_batch')
      end

      Array.new(texts_array.size)
    end
  end

  private

  def initialize_client
    @client = Aws::BedrockRuntime::Client.new(
      region: @region,
      # Increase timeout for Bedrock API calls
      http_read_timeout: 60,
      retry_limit: 3
    )

    @logger.info "Initialized Bedrock client in region: #{@region}"

  rescue => e
    @logger.error "Failed to initialize Bedrock client: #{e.message}"
    @logger.error "Translation will be disabled. Transcription will continue without translation."

    Sentry.capture_exception(e) do |scope|
      scope.set_tag('component', 'bedrock_client_init')
    end

    @enabled = false
    # Don't raise - allow service to continue without translation
  end

  def build_translation_prompt(text, source_lang, target_lang)
    lang_names = {
      'ja' => 'Japanese',
      'en' => 'English',
      'zh' => 'Chinese',
      'ko' => 'Korean',
      'es' => 'Spanish',
      'fr' => 'French'
    }

    source_name = lang_names[source_lang] || source_lang
    target_name = lang_names[target_lang] || target_lang

    # Create a focused translation prompt
    <<~PROMPT
      Translate the following #{source_name} text to #{target_name}.
      Provide only the translation without any explanation or additional text.
      Maintain the tone and style of the original text.

      Text to translate:
      #{text}

      Translation:
    PROMPT
  end

  def extract_translation(response_body)
    # Extract text from Claude's response
    content = response_body['content']
    return nil unless content && content.is_a?(Array) && !content.empty?

    # Get the text from the first content block
    text_content = content.find { |c| c['type'] == 'text' }
    return nil unless text_content

    # Clean up the translation (remove any leading/trailing whitespace)
    translation = text_content['text'].strip

    # Sometimes the model might include quotes or explanation, try to extract just the translation
    # Remove common prefixes if present
    translation.sub!(/^(Translation:|Translated text:|Here's the translation:)\s*/i, '')
    translation.strip
  end

  def add_to_cache(key, value)
    # Implement simple LRU-like cache management
    if @translation_cache.size >= @cache_max_size
      # Remove oldest entries (simple approach - remove first 20% of entries)
      keys_to_remove = @translation_cache.keys.first(@cache_max_size / 5)
      keys_to_remove.each { |k| @translation_cache.delete(k) }

      @logger.debug "Cache pruned, removed #{keys_to_remove.size} entries" if ENV['DEBUG']
    end

    @translation_cache[key] = value
  end

  def build_batch_translation_prompt(texts_to_translate, source_lang, target_lang)
    lang_names = {
      'ja' => 'Japanese',
      'en' => 'English',
      'zh' => 'Chinese',
      'ko' => 'Korean',
      'es' => 'Spanish',
      'fr' => 'French'
    }

    source_name = lang_names[source_lang] || source_lang
    target_name = lang_names[target_lang] || target_lang

    texts_formatted = texts_to_translate.map.with_index do |item, i|
      "[#{i + 1}] #{item[:text]}"
    end.join("\n")

    <<~PROMPT
      Translate the following #{texts_to_translate.size} #{source_name} texts to #{target_name}.
      Provide only the translations, each on a new line with its number.
      Maintain the tone and style of each original text.
      Format: [number] translated text

      Texts to translate:
      #{texts_formatted}

      Translations:
    PROMPT
  end

  def extract_batch_translations(response_body, expected_count)
    content = response_body['content']
    return Array.new(expected_count) unless content && content.is_a?(Array) && !content.empty?

    text_content = content.find { |c| c['type'] == 'text' }
    return Array.new(expected_count) unless text_content

    # Parse numbered translations
    translations = Array.new(expected_count)
    lines = text_content['text'].strip.split("\n")

    lines.each do |line|
      # Match pattern: [number] translation
      if match = line.match(/^\[(\d+)\]\s*(.+)$/)
        index = match[1].to_i - 1
        translation = match[2].strip
        translations[index] = translation if index >= 0 && index < expected_count
      end
    end

    translations
  end
end
