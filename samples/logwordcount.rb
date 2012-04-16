#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'

cascade 'logwordcount' do
  flow 'logwordcount' do
    # This works just as well, but will get you blocked by Project Gutenberg
    #source 'input', tap('http://www.gutenberg.org/files/20417/20417-8.txt')
    source 'input', tap('samples/data/gutenberg/the_outline_of_science_vol_1')

    assembly 'input' do
      # TODO: create a helper for RegexSplitGenerator
      each 'line', :function => regex_split_generator('word', :pattern => /[.,]*\s+/)
      group_by 'word' do
        count
      end
      group_by 'count', :reverse => true
    end

    sink 'input', tap('output/logwordcount', :sink_mode => :replace)
  end
end.complete(local_properties('build/sample'))
