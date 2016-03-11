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

    KNOWN_ERRORS = ['LeaseExpiredException'].freeze

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
      @retry_known_errors = options[:retry_known_errors]
      @retry_times = options[:retry_times]
      @retry_interval = options[:retry_interval]
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

    def generic_request(connection, request_path, method, header = nil,
                        payload = nil)
      res = nil
      req = Net::HTTPGenericRequest.new(method, (payload ? true : false),
                                        true, request_path, header)
      raise WebHDFS::ClientError, 'Error accepting given IO resource as' \
       ' data payload, Not valid in methods' \
       ' other than PUT and POST' unless method == 'PUT' || method == 'POST'

      req.body_stream = payload
      req.content_length = payload.size
      begin
        res = connection.request(req)
      rescue => e
        raise WebHDFS::ServerError, 'Failed to connect to host' \
                                    " #{@host}:#{@port}, #{e.message}"
      end
      res
    end

    def make_request(connection, request_path, method, header = nil,
                     payload = nil)
      res = nil
      if !payload.nil? && payload.respond_to?(:read) &&
         payload.respond_to?(:size)
        res = generic_request(connection, request_path, method, header, payload)
      else
        begin
          res = connection.send_request(method, request_path, payload, header)
        rescue => e
          raise WebHDFS::ServerError, 'Failed to connect to host' \
                                      " #{@host}:#{@port}, #{e.message}"
        end
      end
      res
    end

    def raise_response_error(code, message)
      case code
      when '400'
        raise WebHDFS::ClientError, message
      when '401'
        raise WebHDFS::SecurityError, message
      when '403'
        raise WebHDFS::IOError, message
      when '404'
        raise WebHDFS::FileNotFoundError, message
      when '500'
        raise WebHDFS::ServerError, message
      else
        raise WebHDFS::RequestFailedError, "response code:#{code}, " \
                                           "message:#{message}"
      end
    end

    # Execute request
    def execute(path, method, header = nil, payload = nil, op = nil,
                params = {}, retries = 0)
      conn = connection

      header = @kerberos.autorization(header) if @kerberos

      request_path = build_path(path, op, params)

      response = make_request(conn, payload)

      @kerberos.check_response(response) if @kerberos

      case response
      when Net::HTTPSuccess
        response
      when Net::HTTPRedirection
        response
      else
        message = if response.body && !response.body.empty?
                    response.body.delete("\n")
                  else
                    'Response body is empty...'
                  end
        if @retry_known_errors && retries < @retry_times
          detail = nil
          if message =~ /^\{"RemoteException":\{/
            begin
              detail = JSON.parse(message)

              if detail['RemoteException'] &&
                 KNOWN_ERRORS.include?(detail['RemoteException']['exception'])
                sleep @retry_interval if @retry_interval > 0
                return execute(path, method, header, payload, op, params,
                               retries + 1)
              end
            rescue
              # ignore broken json response body
            end
          end
        end
        raise_response_error(response.code, message)
      end
    end
  end
end
