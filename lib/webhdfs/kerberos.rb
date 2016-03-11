require 'base64'
require 'gssapi'

require_relative 'exceptions'

module WebHDFS
  # Kerberos class for http requests
  class Kerberos
    attr_read :host, :keytab, :token

    # Constructor
    def initialize(host, keytab)
      @host = host
      @keytab = keytab

      @gsscli = GSSAPI::Simple.new(@host, 'HTTP', @kerberos_keytab)
      @token = nil
      begin
        @token = @gsscli.init_context
      rescue => token_error
        raise WebHDFS::KerberosError, token_error.message
      end
    end

    # Set the token to header authorization
    def authorization(header)
      encoded_token = Base64.strict_encode64(@token)
      if header
        header['Authorization'] = "Negotiate #{encoded_token}"
      else
        header = { 'Authorization' => "Negotiate #{encoded_token}" }
      end
      header
    end

    def check_response(response)
      if @kerberos && response.code == '307'
        itok = (response.header.get_fields('WWW-Authenticate') ||
                ['']).pop.split(/\s+/).last
        unless itok
          raise WebHDFS::KerberosError, 'Server does not return ' \
                                        'WWW-Authenticate header'
        end
        begin
          @gsscli.init_context(Base64.strict_decode64(itok))
        rescue => e
          raise WebHDFS::KerberosError, e.message
        end
      end
    end
  end
end
