require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe Object do
  it 'should allow expr syntax' do
    test_assembly do
      insert 'foo' => 1, 'bar' => expr('offset:int')
      check_scope :values_fields => ['offset', 'line', 'bar', 'foo']
    end
  end

  it 'should compile expressions' do
    e = ExprStub.new('x:int + y:int')
    e.compile
  end

  it 'should throw an exception for parsing failures' do
    e = ExprStub.new('x:int + doesnotparse y:string')
    lambda{ e.compile }.should raise_error ExprParseException
  end

  it 'should throw an exception for compile failures' do
    e = ExprStub.new('new DoesNotExist(x:int).doesnotcompile()')
    lambda{ e.compile }.should raise_error ExprCompileException
  end

  it 'should throw an exception for compile failures' do
    e = ExprStub.new('true ? x:int : y:string')
    lambda{ e.compile }.should raise_error ExprCompileException
  end
end
