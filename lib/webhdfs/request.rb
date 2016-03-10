require 'net/http'
require 'uri'
require 'json'
require 'addressable/uri'
require 'openssl'

require_relative 'utilities'
require_relative 'exceptions'

module WebHDFS
  # Class to make http requests
  class Request
    attr_reader :host, :port, :username, :doas
    attr_reader :proxy, :ssl, :kerberos
    attr_reader :open_timeout, :read_timeout

    # Constructor
    def initialize(host, port, options = {})
      @host = host
      @port = port
      @username = options[:username]
      @doas = options[:doas]
      @proxy = options[:proxy]
      @ssl = options[:ssl]
      @kerberos = options[:kerberos]
      @open_timeout = options[:open_timeout]
      @read_timeout = options[:read_timeout]
    end

    def connection
      conn = if @proxy
               Net::HTTP.new(host, port, @proxy.address, @proxy.port)
             else
               Net::HTTP.new(host, port)
             end

      if @proxy.authentication?
        conn.proxy_user = @proxy.user
        conn.proxy_pass = @proxy.password
      end

      conn.open_timeout = @open_timeout if @open_timeout
      conn.read_timeout = @read_timeout if @read_timeout

      if @ssl
        @ssl.apply_to(conn)
      else
        conn
      end
    end

    def build_path(path, op, params)
      path = Addressable::URI.escape(path)
      if op
        opts = if @username && @doas
                 { 'op' => op, 'user.name' => @username, 'doas' => @doas }
               elsif @username
                 { 'op' => op, 'user.name' => @username }
               elsif @doas
                 { 'op' => op, 'doas' => @doas }
               else
                 { 'op' => op }
               end
        WebHDFS.api_path(path) + '?' + URI.encode_www_form(params.merge(opts))
      else
        path
      end
    end

    # Execute request
    def execute(path, _method, header = nil, op = nil, params = {})
      conn = connection

      header = @kerberos.autorization(header) if @kerberos

      request_path = build_path(path, op, params)
    end
  end
end
