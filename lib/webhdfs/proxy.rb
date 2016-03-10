module WebHDFS
  # Proxy class for http requests
  class Proxy
    attr_reader :address, :port, :user, :password

    # Constructor
    def initialize(address, port)
      @address = address
      @port = port
      @user = @password = nil
    end

    # Set authentication credentials
    def credentials(user, password)
      @user = user
      @password = password
    end

    # Proxy has authentication?
    def authentication?
      if @user && @password
        true
      else
        false
      end
    end
  end
end
