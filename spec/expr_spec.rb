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

  it 'should evaluate expressions' do
    e = ExprStub.new('x:int + y:int')
    result = e.evaluate([2, 3])
    result.should == 5
  end

  it 'should throw an exception for invalid actual parameters' do
    e = ExprStub.new('x:int + y:int')
    lambda{ e.evaluate([2, 'blah']) }.should raise_error ExprArgException
  end

  it 'should throw an exception for missing actual parameters' do
    e = ExprStub.new('x:int + y:int')
    lambda{ e.evaluate([2]) }.should raise_error ExprArgException
  end

  it 'should throw an exception for null actual parameters' do
    e = ExprStub.new('x:int + y:int')
    lambda{ e.evaluate([2, nil]) }.should raise_error ExprArgException
  end

  it 'should use default actual parameters to test evaluation' do
    e = ExprStub.new('x:int + y:int')
    result = e.test_evaluate
    result.should == 0

    e = ExprStub.new('x:int + y:string')
    result = e.test_evaluate
    result.should == '0'

    e = ExprStub.new('x:long + y:int')
    result = e.test_evaluate
    result.should == 0

    e = ExprStub.new('x:double + y:int')
    result = e.test_evaluate
    result.should == 0.0

    e = ExprStub.new('x:float + y:int')
    result = e.test_evaluate
    result.should == 0.0

    e = ExprStub.new('x:bool && y:bool')
    result = e.test_evaluate
    result.should == false
  end
end
