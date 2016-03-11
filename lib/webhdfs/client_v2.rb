require 'net/http'
require 'uri'
require 'json'
require 'addressable/uri'

require_relative 'utilities'
require_relative 'exceptions'
require_relative 'proxy'
require_relative 'ssl'
require_relative 'kerberos'

module WebHDFS
  class ClientV2
    attr_accessor :host, :port, :username, :doas, :proxy
    attr_accessor :open_timeout # default 30s (in ruby net/http)
    attr_accessor :read_timeout # default 60s (in ruby net/http)
    attr_accessor :httpfs_mode
    attr_accessor :retry_known_errors # default false (not to retry)
    attr_accessor :retry_times        # default 1 (ignored when retry_known_errors is false)
    attr_accessor :retry_interval     # default 1 ([sec], ignored when retry_known_errors is false)
    attr_accessor :ssl
    attr_accessor :kerberos
    attr_accessor :http_headers

    # This hash table holds command options.
    OPT_TABLE = {}.freeze # internal use only
    OPT_TABLE['CREATE'] = %w(overwrite blocksize replication permission
                             buffersize data)
    OPT_TABLE['APPEND'] = %w(buffersize data)
    OPT_TABLE['OPEN'] = %w(offset length buffersize)
    OPT_TABLE['MKDIRS'] = ['permission']
    OPT_TABLE['DELETE'] = ['recursive']
    OPT_TABLE['SETOWNER'] = %w(owner group)
    OPT_TABLE['SETTIMES'] = %w(modificationtime accesstime)

    REDIRECTED_OPERATIONS = %w(APPEND CREATE OPEN GETFILECHECKSUM).freeze

    def initialize(host = 'localhost', port = 50_070, username = nil,
                   doas = nil, proxy_address = nil, proxy_port = nil,
                   http_headers = {})
      @host = host
      @port = port
      @username = username
      @doas = doas

      @proxy = WebHDFS::Proxy.new(proxy_address, proxy_port) if proxy_address && proxy_port

      @retry_known_errors = false
      @retry_times = @retry_interval = 1

      @httpfs_mode = false

      @ssl = nil

      @kerberos = nil
      @http_headers = http_headers
    end

    def request_options
      {
        username: @username,
        doas: @doas,
        proxy: @proxy,
        ssl: @ssl,
        kerberos: @kerberos,
        open_timeout: @open_timeout, read_timeout: @read_timeout,
        retry_known_errors: @retry_known_errors,
        retry_times: @retry_times, retry_interval: @retry_interval
      }
    end

    # curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
    #                 [&overwrite=<true|false>][&blocksize=<LONG>]
    #                 [&replication=<SHORT>]
    #                 [&permission=<OCTAL>][&buffersize=<INT>]"
    def create(path, body, options = {})
      options = options.merge('data' => 'true') if @httpfs_mode
      WebHDFS.check_options(options, OPT_TABLE['CREATE'])
      res = operate_requests('PUT', path, 'CREATE', options, body)
      res.code == '201'
    end

    # curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=
    #                      APPEND[&buffersize=<INT>]"
    def append(path, body, options = {})
      options = options.merge('data' => 'true') if @httpfs_mode
      WebHDFS.check_options(options, OPT_TABLE['APPEND'])
      res = operate_requests('POST', path, 'APPEND', options, body)
      res.code == '200'
    end

    # curl -i -L "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN
    #                [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]"
    def read(path, options = {})
      WebHDFS.check_options(options, OPT_TABLE['OPEN'])
      res = operate_requests('GET', path, 'OPEN', options)
      res.body
    end

    alias open read

    # curl -i -X PUT "http://<HOST>:<PORT>/<PATH>?op=
    #                     MKDIRS[&permission=<OCTAL>]"
    def mkdir(path, options = {})
      WebHDFS.check_options(options, OPT_TABLE['MKDIRS'])
      res = operate_requests('PUT', path, 'MKDIRS', options)
      WebHDFS.check_success_json(res, 'boolean')
    end

    alias mkdirs mkdir

    # curl -i -X PUT "<HOST>:<PORT>/webhdfs/v1/<PATH>?op=
    #                       RENAME&destination=<PATH>"
    def rename(path, dest, options = {})
      WebHDFS.check_options(options, OPT_TABLE['RENAME'])
      dest = '/' + dest unless dest.start_with?('/')
      res = operate_requests('PUT', path, 'RENAME',
                             options.merge('destination' => dest))
      WebHDFS.check_success_json(res, 'boolean')
    end

    # curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE
    #                          [&recursive=<true|false>]"
    def delete(path, options = {})
      WebHDFS.check_options(options, OPT_TABLE['DELETE'])
      res = operate_requests('DELETE', path, 'DELETE', options)
      WebHDFS.check_success_json(res, 'boolean')
    end

    # curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS"
    def stat(path, options = {})
      WebHDFS.check_options(options, OPT_TABLE['GETFILESTATUS'])
      res = operate_requests('GET', path, 'GETFILESTATUS', options)
      WebHDFS.check_success_json(res, 'FileStatus')
    end
    alias getfilestatus stat

    # curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"
    def list(path, options = {})
      WebHDFS.check_options(options, OPT_TABLE['LISTSTATUS'])
      res = operate_requests('GET', path, 'LISTSTATUS', options)
      WebHDFS.check_success_json(res, 'FileStatuses')['FileStatus']
    end
    alias liststatus list

    # curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETCONTENTSUMMARY"
    def content_summary(path, options = {})
      WebHDFS.check_options(options, OPT_TABLE['GETCONTENTSUMMARY'])
      res = operate_requests('GET', path, 'GETCONTENTSUMMARY', options)
      WebHDFS.check_success_json(res, 'ContentSummary')
    end
    alias getcontentsummary content_summary

    # curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM"
    def checksum(path, options = {})
      WebHDFS.check_options(options, OPT_TABLE['GETFILECHECKSUM'])
      res = operate_requests('GET', path, 'GETFILECHECKSUM', options)
      WebHDFS.check_success_json(res, 'FileChecksum')
    end
    alias getfilechecksum checksum

    # curl -i "http://<HOST>:<PORT>/webhdfs/v1/?op=GETHOMEDIRECTORY"
    def homedir(options = {})
      WebHDFS.check_options(options, OPT_TABLE['GETHOMEDIRECTORY'])
      res = operate_requests('GET', '/', 'GETHOMEDIRECTORY', options)
      WebHDFS.check_success_json(res, 'Path')
    end
    alias gethomedirectory homedir

    # curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETPERMISSION
    #                 [&permission=<OCTAL>]"
    def chmod(path, mode, options = {})
      WebHDFS.check_options(options, OPT_TABLE['SETPERMISSION'])
      res = operate_requests('PUT', path, 'SETPERMISSION',
                             options.merge('permission' => mode))
      res.code == '200'
    end
    alias setpermission chmod

    # curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETOWNER
    #                          [&owner=<USER>][&group=<GROUP>]"
    def chown(path, options = {})
      WebHDFS.check_options(options, OPT_TABLE['SETOWNER'])
      unless options.key?('owner') || options.key?('group') ||
             options.key?(:owner) || options.key?(:group)
        raise ArgumentError, "'chown' needs at least one of owner or group"
      end
      res = operate_requests('PUT', path, 'SETOWNER', options)
      res.code == '200'
    end

    alias setowner chown

    # curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETREPLICATION
    #                           [&replication=<SHORT>]"
    def replication(path, replnum, options = {})
      WebHDFS.check_options(options, OPT_TABLE['SETREPLICATION'])
      res = operate_requests('PUT', path, 'SETREPLICATION',
                             options.merge('replication' => replnum.to_s))
      WebHDFS.check_success_json(res, 'boolean')
    end
    alias setreplication replication

    # curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETTIMES
    #                           [&modificationtime=<TIME>][&accesstime=<TIME>]"
    # motidicationtime: radix-10 logn integer
    # accesstime: radix-10 logn integer
    def touch(path, options = {})
      WebHDFS.check_options(options, OPT_TABLE['SETTIMES'])
      unless options.key?('modificationtime') || options.key?('accesstime') ||
             options.key?(:modificationtime) || options.key?(:accesstime)
        raise ArgumentError, "'chown' needs at least one of " \
                               'modificationtime or accesstime'
      end
      res = operate_requests('PUT', path, 'SETTIMES', options)
      res.code == '200'
    end

    alias settimes touch

    def operate_requests(method, path, op, params = {}, payload = nil)
      request = WebHDFS::Request.new(@host, @port, request_options)
      if !@httpfs_mode && REDIRECTED_OPERATIONS.include?(op)
        response = request.execute(path, method, nil, nil, op, params, nil)
        unless response.is_a?(Net::HTTPRedirection) && response['location']
          msg = 'NameNode returns non-redirection (or without location' \
                " header), code:#{res.code}, body:#{res.body}."
          raise WebHDFS::RequestFailedError, msg
        end
        uri = URI.parse(response['location'])
        rpath = if uri.query
                  uri.path + '?' + uri.query
                else
                  uri.path
                end
        request = WebHDFS::Request.new(uri.host, uri.port, request_options)
        request.execute(rpath, method,
                        { 'Content-Type' => 'application/octet-stream' },
                        payload, nil, {})
      elsif @httpfs_mode && !payload.nil?
        request.execute(path, method,
                        { 'Content-Type' => 'application/octet-stream' },
                        payload, op, params)
      else
        request.execute(path, method, nil, payload, op, params)
      end
    end
  end
end
