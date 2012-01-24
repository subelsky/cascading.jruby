namespace :ant do
  desc 'Builds Java source for inclusion in gem'
  task :build do
    stdout = `ant build`
    raise "Ant build failed: #{stdout}" unless $? == 0
    puts stdout
  end

  desc 'Cleans Java build files'
  task :clean do
    stdout = `ant clean`
    puts stdout
  end
end
