# Hacking

Some hacking info on `cascading.jruby`:

For local development, install with (requires [bundler](http://gembundler.com/)):

    bundle install

To run the tests (will download Cascading and Hadoop jars):

    jruby -S bundle exec rake

To create the gem:

    jruby -S bundle exec rake gem

To install it locally:

    jruby -S gem install pkg/cascading.jruby-xxx.gem

The `Cascading::Operations` module is mixed-in the `Cascading::Assembly` class to provide some shortcuts for common operations.

The file cascading/cascading.rb defines global helper methods for cascading like tap creation, fields creation, etc. 
