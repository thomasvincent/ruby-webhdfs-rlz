module WebHDFS
  class Error < StandardError; end

  class FileNotFoundError < WebHDFS::Error; end

  class IOError < WebHDFS::Error; end
  class SecurityError < WebHDFS::Error; end

  class ClientError < WebHDFS::Error; end
  class ServerError < WebHDFS::Error; end

  class RequestFailedError < WebHDFS::Error; end

  class KerberosError < WebHDFS::Error; end
end
