#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'

cascade 'copy', :mode => :local do
  flow 'copy' do
    # You don't have to curl and cache inputs: tap can fetch via HTTP
    #source 'input', tap('http://www.census.gov/genealogy/names/dist.all.last')
    source 'input', tap('samples/data/genealogy/names/dist.all.last')

    assembly 'input' do
      rename 'line' => 'value'
      # We override validate_with because we know line will never be null
      reject 'value:string.indexOf("R") == -1', :validate_with => { :value => 'nothinghere' }
    end

    sink 'input', tap('output/copy', :sink_mode => :replace)
  end
end.complete(local_properties('build/sample'))
