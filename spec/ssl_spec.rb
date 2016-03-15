require_relative '../lib/webhdfs/ssl'

RSpec.describe WebHDFS::SSL do
  before(:each) do
    @ssl = WebHDFS::SSL.new ca_file: 'file', verify_mode: :none, version: 3
  end

  subject { @ssl }

  it { expect(subject).to respond_to(:ca_file) }
  it { expect(subject).to respond_to(:verify_mode) }
  it { expect(subject).to respond_to(:version) }
  it { expect(subject).to respond_to(:cert) }
  it { expect(subject).to respond_to(:key) }

  describe '#verify_mode' do
    it 'valid value' do
      valid_values = [:none, :peer]

      valid_values.each do |value|
        expect { @ssl.verify_mode = value }.to_not raise_error
      end
    end

    it 'invalid value' do
      invalid_values = [:no, :pee, :yes]

      invalid_values.each do |value|
        expect { @ssl.verify_mode = value }.to raise_error ArgumentError
      end
    end
  end

  describe '#apply_to' do
    # TODO: pending need mock some objects
  end
end
