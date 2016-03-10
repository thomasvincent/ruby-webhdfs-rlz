if RUBY_VERSION =~ /^1\.8\./
  require 'cgi'

  def require_relative(relative_feature)
    file = caller.first.split(/:\d/, 2).first
    raise LoadError, "require_relative is called in #{Regexp.last_match(1)}"\
                     if /\A\((.*)\)/ =~ file
    require File.expand_path(relative_feature, File.dirname(file))
  end

  module URI
    def self.encode_www_form(enum)
      enum.map do |key, value|
        if value.nil?
          CGI.escape(key)
        elsif value.respond_to?(:to_ary)
          value.to_ary.map do |w|
            str = CGI.escape(key)
            str << '=' << CGI.escape(w) unless w.nil?
          end.join('&')
        else
          CGI.escape(key.to_s) << '=' << CGI.escape(value.to_s)
        end
      end.join('&')
    end
  end
end
