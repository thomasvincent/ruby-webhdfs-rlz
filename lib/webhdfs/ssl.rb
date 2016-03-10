require 'openssl'

module WebHDFS
  # SSL class for http requests
  class SSL
    attr_reader :ca_file, :verify_mode
    attr_reader :cert, :key, :version
    SSL_VERIFY_MODES = [:none, :peer].freeze

    # Constructor
    def initialize(options = {})
      @ca_file = options[:ca_file]
      self.verify_mode = options[:verify_mode]
      @cert = options[:cert]
      @key = options[:key]
      @version = options[:version]
    end

    # Verify valid ssl mode
    def verify_mode=(mode)
      unless SSL_VERIFY_MODES.include? mode
        raise ArgumentError, "Invalid SSL verify mode #{mode.inspect}"
      end
      @verify_mode = mode
    end

    # Apply ssl to a http connection
    def apply_to(connection)
      connection.use_ssl = true
      connection.ca_file = @ca_file if @ca_file
      if @verify_mode
        connection.verify_mode = case @verify_mode
                                 when :none then OpenSSL::SSL::VERIFY_NONE
                                 when :peer then OpenSSL::SSL::VERIFY_PEER
                                 end
      end
      connection.cert = @cert if @cert
      connection.key = @key if @key
      connection.ssl_version = @version if @version

      connection
    end
  end
end
