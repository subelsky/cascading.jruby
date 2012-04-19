namespace :samples do
  desc 'Run all sample applications'
  task :run do
    Dir.glob('samples/*.rb') do |sample|
      next unless File.executable?(sample)
      success = system(sample)
      raise "#{sample} sample app failed" unless success
    end
  end

  desc 'Remove sample outputs and build artifacts (also cleans specs)'
  task :clean do
    `rm -rf output`
    `rm -rf build`
  end
end

desc 'Alias to samples:run'
task :samples => 'samples:run'
