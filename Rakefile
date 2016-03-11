require 'bundler'
require 'bundler/gem_tasks'

# Default directory to look in is `/specs`
# Run with `rake spec`
RSpec::Core::RakeTask.new(:spec) do |task|
  task.rspec_opts = ['--color', '--format', 'nested']
end

task default: :spec

Bundler::GemHelper.install_tasks

require 'rake/testtask'

Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.test_files = FileList['test/webhdfs/*.rb']
  test.verbose = true
end

task :doc do |_t|
  command = 'bundle exec rdoc --markup=tomdoc --visibility=public ' \
            '--include=lib --exclude=test'
  `#{command}`
end

task :coverage do |_t|
  ENV['SIMPLE_COV'] = '1'
  Rake::Task['test'].invoke
end
