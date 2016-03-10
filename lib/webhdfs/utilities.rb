require 'json'

# Principal module
module WebHDFS
  # Path to access API
  def self.api_path(path)
    if path.start_with?('/')
      '/webhdfs/v1' + path
    else
      '/webhdfs/v1/' + path
    end
  end

  # Check if json request is success
  def self.check_success_json(res, attr = nil)
    res.code == '200' && res.content_type == 'application/json' &&
      (attr.nil? || JSON.parse(res.body)[attr])
  end

  # Check if options are valid
  def self.check_options(options, optdecl = [])
    ex = options.keys.map(&:to_s) - (optdecl || [])
    raise ArgumentError, "no such option: #{ex.join(' ')}" unless ex.empty?
  end
end
