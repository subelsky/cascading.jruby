namespace :ant do
  desc 'Retrieves Cascading and Hadoop jars and sets environment variables to point to them'
  task :retrieve do
    raise 'Ant retrieve failed' unless system('ant retrieve')
    ENV['CASCADING_HOME'] = 'build/lib'
    ENV['HADOOP_HOME'] = 'build/lib'
  end

  desc 'Builds Java source for inclusion in gem'
  task :build do
    raise 'Ant build failed' unless system('ant build')
  end

  desc 'Cleans Java build files'
  task :clean do
    system('ant clean')
  end
end
