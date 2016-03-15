require_relative '../lib/webhdfs/request'

RSpec.describe WebHDFS::Request do
  before(:each) do
    @request = WebHDFS::Request.new '127.0.0.1', 80
  end

  subject { @request }

  it { expect(subject).to respond_to(:host) }
  it { expect(subject).to respond_to(:port) }
  it { expect(subject).to respond_to(:username) }
  it { expect(subject).to respond_to(:doas) }
  it { expect(subject).to respond_to(:proxy) }
  it { expect(subject).to respond_to(:ssl) }
  it { expect(subject).to respond_to(:kerberos) }
  it { expect(subject).to respond_to(:open_timeout) }
  it { expect(subject).to respond_to(:read_timeout) }

  describe '#raise_response_error' do
    it 'ClientError' do
      expect { @request.raise_response_error '400', 'message' }.to \
        raise_error WebHDFS::ClientError
    end

    it 'SecurityError' do
      expect { @request.raise_response_error '401', 'message' }.to \
        raise_error WebHDFS::SecurityError
    end

    it 'IOError' do
      expect { @request.raise_response_error '403', 'message' }.to \
        raise_error WebHDFS::IOError
    end

    it 'FileNotFoundError' do
      expect { @request.raise_response_error '404', 'message' }.to \
        raise_error WebHDFS::FileNotFoundError
    end

    it 'ServerError' do
      expect { @request.raise_response_error '500', 'message' }.to \
        raise_error WebHDFS::ServerError
    end

    it 'RequestFailedError' do
      expect { @request.raise_response_error '000', 'message' }.to \
        raise_error WebHDFS::RequestFailedError
    end
  end
end
