require_relative '../lib/webhdfs/proxy'

RSpec.describe WebHDFS::Proxy do
  before(:each) do
    @proxy = WebHDFS::Proxy.new '127.0.0.1', 3128
  end

  subject { @proxy }

  it { expect(subject).to respond_to(:address) }
  it { expect(subject).to respond_to(:port) }
  it { expect(subject).to respond_to(:user) }
  it { expect(subject).to respond_to(:password) }

  describe '#credentials' do
    it 'Before credentials call' do
      proxy = WebHDFS::Proxy.new '127.0.0.1', 3128

      expect(proxy.user).to be_nil
      expect(proxy.password).to be_nil
    end

    it 'After credentials call' do
      proxy = WebHDFS::Proxy.new '127.0.0.1', 3128
      proxy.credentials('user', 'password')

      expect(proxy.user).to eq('user')
      expect(proxy.password).to eq('password')
    end
  end

  describe '#authentication' do
    it 'Before credentials call' do
      proxy = WebHDFS::Proxy.new '127.0.0.1', 3128

      expect(proxy.authentication?).to be false
    end

    it 'After credentials call' do
      proxy = WebHDFS::Proxy.new '127.0.0.1', 3128
      proxy.credentials('user', 'password')

      expect(proxy.authentication?).to be true
    end
  end
end
